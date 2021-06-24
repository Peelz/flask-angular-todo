import sys
import os
import redis
import json
import copy

from flask import request, jsonify
from flask_api import status
from flask import current_app as app
from datetime import datetime

from app.commons.abstract_activity import AbstractActivity
from app.commons import constants
from app.commons.getcache import GetcacheViews
from app.commons.services.cache import Cache

class ConfigurationViews(AbstractActivity):

    def get(self):
        try:
            dataset_name = getattr(constants, 'DATASET_NAME', None)
            activity_configuration = copy.deepcopy(getattr(constants, 'ACTIVITY_CONFIGURATION', dict()))
            template_attributes = getattr(constants, 'TEMPLATE_ATTRIBUTES', dict())
            reference_fields = list()
            contains_ignore_fields = list()

            activities = activity_configuration.get('activities') or dict()
            for key, value in activities.items():
                if key == 'search':
                    sections = value.get('sections') or list()

                    for section in sections:
                        if section.get('section_type') == 'criteria':
                            fields = section.get('fields') or list()
                            for columns in fields:
                                for column in columns:
                                    field_name = column.get('field_name')
                                    component_type = column.get('component', dict()).get('code')
                                    component_attibutes = column.get('component', dict()).get('attributes') or dict()

                                    if component_type == 'widget_master':
                                        reference_fields.append(field_name)

                                    if component_type == 'widget_textbox':
                                        if 'encrypt-field' in component_attibutes and (not component_attibutes.get('encrypt-field') or component_attibutes.get('encrypt-field') == '' or component_attibutes.get('encrypt-field') == 'false'):
                                            contains_ignore_fields.append(field_name)

            current_reference_uuid = self.flow_tracking.get('current_reference_uuid')
            self.logger.debug('ConfigurationViews.get.current_reference_uuid: {}'.format(current_reference_uuid))
            instances = self.flow_tracking.get('instances') or list()
            self.logger.debug('ConfigurationViews.get.instances: {}'.format(instances))
            getcacheViews = GetcacheViews()
            instance = getcacheViews.getInstance(current_reference_uuid, instances)
            self.logger.debug('ConfigurationViews.get.instance: {}'.format(instance))
            auto_next = False
            if 'auto_next' in instance:
                auto_next = instance['auto_next']
            auto_gen_fields = []
            if instance.get('instance_type') != 'custom':
                if dataset_name:
                    meta_fields = self.getMetaData_2(self.launcher_service, dataset_name)
                    for field in meta_fields:
                        if field.get('auto_gen'):
                            auto_gen_fields.append(field.get('name', None))
            
            response_intent = None
            try:
                response_intent = GetcacheViews().post()
            except Exception as e:
                response_intent = {
                    'internal_server_error': str(e)
                }
            
            self.logger.info('ConfigurationViews - response_intent: {}'.format(response_intent))
            
            self.logger.debug('ConfigurationViews.response_intent: {}'.format(response_intent))

            self.logger.debug('ConfigurationViews.get.app.config: {}'.format(app.config))

            enc_req = False
            if app.config.get('ENC_REQ') == 'Y':
                enc_req = True
        
            enc_k = []
            if app.config.get('ENC_K'):
                keys = app.config.get('ENC_K').split(',')
                clean_keys = []
                for x in keys:
                    clean_keys.append(x.strip())
                enc_k = clean_keys

            result = {
                'activity_configuration': activity_configuration,
                'template_attributes': template_attributes,
                'reference_fields': reference_fields,
                'contains_ignore_fields': contains_ignore_fields,
                'business_key_gen_fields': getattr(constants, 'BUSINESS_KEY_GEN_FIELDS', []),
                'autofill_fields': getattr(constants, 'AUTOFILL_FIELDS', []),
                'dataset_name': dataset_name,
                'business_key_fields': getattr(constants, 'BUSINESS_KEY_FIELDS', []),
                'instance_code': getattr(constants, 'INSTANCE_CODE', None),
                'instance_name': getattr(constants, 'INSTANCE_NAME', None),
                'display_label': instance.get('display_label', None),
                'group_key_fields': getattr(constants, 'GROUP_KEY_FIELDS', []),
                'auto_next': auto_next,
                'compound_setting_datasets': getattr(constants, 'COMPOUND_SETTING_DATASETS', []),
                'instance_type': instance.get('instance_type', None),
                'auto_gen_fields': auto_gen_fields,
                'datasets_name_version': getattr(constants, 'DATASETS_NAME_VERSION', []),
                'intent': response_intent,
                'enc_req': enc_req,
                'enc_k': enc_k,
                'app_code': self.app_code
            }

            cache_api = Cache()
            key = 'FLOWADJUSTMENT_' + self.flow_uuid
            try:
                response_cache = cache_api.get_json(key)
            except Exception:
                response_cache = None
            
            self.logger.debug('ConfigurationViews.get.response_cache: {}'.format(response_cache))

            if response_cache and current_reference_uuid in response_cache:
                result["activity_configuration"]["activities"] = response_cache[current_reference_uuid]

                adjust_contains_ignore_fields = []

                activities = result["activity_configuration"]["activities"] or dict()
                for key, value in activities.items():
                    if key == 'search':
                        sections = value.get('sections') or list()

                        for section in sections:
                            if section.get('section_type') == 'criteria':
                                fields = section.get('fields') or list()
                                for columns in fields:
                                    for column in columns:
                                        field_name = column.get('field_name')
                                        component_type = column.get('component', dict()).get('code')
                                        component_attibutes = column.get('component', dict()).get('attributes') or dict()

                                        search_type = column.get('search_type')

                                        if component_type == 'widget_textbox':
                                            if search_type == 'exactly':
                                                pass
                                            elif search_type == 'partial':
                                                adjust_contains_ignore_fields.append(field_name)
                                            else:
                                                if 'encrypt-field' in component_attibutes and (not component_attibutes.get('encrypt-field') or component_attibutes.get('encrypt-field') == '' or component_attibutes.get('encrypt-field') == 'false'):
                                                    adjust_contains_ignore_fields.append(field_name)
                
                result['contains_ignore_fields'] = adjust_contains_ignore_fields

            default_values = instance.get('default_values') or list()

            self.logger.debug('ConfigurationViews.get.default_values: {}'.format(default_values))

            default_value_field_found_in_activitys = []
            default_value_transform = {}
            default_value_fields = []
            
            if default_values:
                current_node_intent_cache = getcacheViews.post()
                for field_dv in default_values:
                    field_key = field_dv["intent_name"]
                    field_value = field_dv["value"]
                    field_type = field_dv.get("intent_type", None)
                    field_defaultValue = self.cast_dynamic_additional(field_value, current_node_intent_cache, False)
                    field_defaultValue = field_defaultValue.strip()
                    default_value_transform[field_key] = field_defaultValue
                    obj_default_value = {
                        'field_key': field_key,
                        'field_type': field_type
                    }
                    default_value_fields.append(obj_default_value)
                    self.logger.debug('ConfigurationViews.get.field_defaultValue.{}: {}'.format(field_key, field_defaultValue))
                    section_create = result["activity_configuration"]["activities"]["create"]["sections"][0]["fields"]
                    self.set_default_value_ref_recursive(section_create, field_key, field_defaultValue, default_value_field_found_in_activitys)
            
            default_value_field_found_in_activitys = set(default_value_field_found_in_activitys)
            self.logger.debug('ConfigurationViews.get.default_value_field_found_in_activitys: {}'.format(default_value_field_found_in_activitys))
            self.logger.debug('ConfigurationViews.get.default_value_transform: {}'.format(default_value_transform))
            self.logger.debug('ConfigurationViews.get.default_value_fields: {}'.format(default_value_fields))

            result['default_value_field_not_found_in_activitys'] = []
            for obj_dv in default_value_fields:
                is_found = False
                for field in default_value_field_found_in_activitys:
                    if obj_dv['field_key'] == field:
                        is_found = True
                        break
                if not is_found:
                    value_dv = default_value_transform[obj_dv['field_key']]
                    if obj_dv['field_type'] == "text" and value_dv:
                        value_dv = str(value_dv)
                    elif obj_dv['field_type'] == "datetime" and value_dv:
                        value_dv = int(value_dv)
                    elif obj_dv['field_type'] == "number" and value_dv:
                        value_dv = int(value_dv)
                    result['default_value_field_not_found_in_activitys'].append({
                        'key': obj_dv['field_key'],
                        'value': value_dv,
                    })
            
            self.logger.debug('ConfigurationViews.get.result[default_value_field_not_found_in_activitys]: {}'.format(result['default_value_field_not_found_in_activitys']))

            intent_config = instance.get('intent') or dict()
            self.logger.debug('ConfigurationViews.get.intent_config: {}'.format(intent_config))

            result['intent_config'] = intent_config

            response = self.ui_response.success('success', self.reference_id, result)

        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            exception_message = exc_type, fname, exc_tb.tb_lineno
            self.logger.error('ConfigurationViews.get.e: {}'.format(e))
            self.logger.error('ConfigurationViews.get.exception_message: {}'.format(exception_message))
            response = self.ui_response.error(str(e), self.reference_id)

        finally:
            return jsonify(response), status.HTTP_200_OK

    def set_default_value_ref_recursive(self, fields, target_key, target_value, default_value_field_found_in_activitys):
        self.logger.debug('ConfigurationViews.set_default_value_ref_recursive.{}.fields: {}'.format(target_key, fields))
        self.logger.debug('ConfigurationViews.set_default_value_ref_recursive.{}.target_key: {}'.format(target_key, target_key))
        self.logger.debug('ConfigurationViews.set_default_value_ref_recursive.{}.target_value: {}'.format(target_key, target_value))
        self.logger.debug('ConfigurationViews.set_default_value_ref_recursive.{}.default_value_field_found_in_activitys: {}'.format(target_key, default_value_field_found_in_activitys))
        for row in fields:
            self.logger.debug('ConfigurationViews.set_default_value_ref_recursive.{}.row: {}'.format(target_key, row))
            for col in row:
                self.logger.debug('ConfigurationViews.set_default_value_ref_recursive.{}.col: {}'.format(target_key, col))
                if col["field_type"] == "field_set":
                    self.set_default_value_ref_recursive(col["component"]["fields"], target_key, target_value, default_value_field_found_in_activitys)
                elif col["field_name"] == target_key:
                    if col["field_type"] != "integer":
                        check_date_format = len(target_value.split('/')) == 1 and len(target_value.split(':')) == 1
                        if col["field_type"] == "datetime" and check_date_format and target_value != "null" and target_value != None:
                            value_store = float(target_value)
                            value_timestamp = value_store / 1000
                            value_obj = datetime.fromtimestamp(value_timestamp)
                            value_date_str = value_obj.strftime("%d/%m/%Y")
                            value_time_str = value_obj.strftime("%H:%M")
                            if col["component"]["attributes"]["mode"] == "time":
                                col["component"]["attributes"]["default-value"] = value_time_str
                            elif col["component"]["attributes"]["mode"] == "date":
                                col["component"]["attributes"]["default-value"] = value_date_str
                            else:
                                col["component"]["attributes"]["default-value"] = value_date_str + ' ' + value_time_str
                        else:
                            clean_space_value = target_value.strip()
                            col["component"]["attributes"]["default-value"] = clean_space_value if clean_space_value != "null" and clean_space_value != None else None
                    else:
                        col["component"]["attributes"]["default-value"] = int(float(target_value)) if target_value != "null" and target_value != None else None
                    default_value_field_found_in_activitys.append(target_key)
                    self.logger.debug('ConfigurationViews.set_default_value_ref_recursive.col["component"]: {}'.format(col["component"]))
                    self.logger.debug('ConfigurationViews.set_default_value_ref_recursive.target_value: {}'.format(target_value))
