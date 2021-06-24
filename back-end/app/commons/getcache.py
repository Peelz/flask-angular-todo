import redis
import json
import os
import sys
import re
import uuid
import time
import pytz

from flask import current_app as app
from flask import jsonify, request

from datetime import datetime

from app.commons.services.cache import Cache
from app.commons.services.log import Logger
from app.commons import constants
from app.commons.global_variable import GlobalVariable
from app.commons.login_data import LoginData

class GetcacheViews():

    disable_extend = True

    def __init__(self):
        self.logger = Logger()

        self.login_data = LoginData()

        self.cache = Cache()
        
        self.workflowkey = self.login_data.get_work_flow_key()

        self.flow_tracking = self.login_data.get_flow_tracking()

    def convertTypeData(self, expectType, value):
        if expectType == 'text':
            value = str(value)
        elif expectType in ['number', 'integer', 'decimal', 'datetime']:
            value_str = str(value)

            if value_str.find('.') == -1:
                value = int(value)
            else:
                value = float(value)
        elif expectType == 'boolean':
            if type(value).__name__ == 'str':
                value = value.lower()
                if value == 'true':
                    value = True
                elif value == 'false':
                    value = False
                else:
                    value = None
            elif type(value).__name__ == 'int':
                value = bool(value)
        elif expectType == 'object':
            if not (isinstance(value, dict)):
                value = json.loads(value)

        return value


    def convertTypeDataForJson(self, expectType, value):
        if expectType == 'text':
            value = "\"" + str(value) + "\""
        elif expectType in ['number', 'integer', 'decimal', 'datetime']:
            if value == '':
                value = None
        elif expectType == 'boolean':
            if type(value).__name__ == 'str':
                value = value.lower()
                if value == 'true':
                    value = True
                elif value == 'false':
                    value = False
                else:
                    value = None
            elif type(value).__name__ == 'int':
                value = bool(value)
        elif expectType == 'object':
            if not (isinstance(value, dict)):
                value = json.loads(value)
        else:
            raise Exception('Not support type {}'.format(expectType))
        
        return value


    def convertObjectToAssociateArray(self, __object):
        hierarchy = __object.split('.')
        for index, node in enumerate(hierarchy):
            parameter = ''
            for i in range(0, index + 1):
                param = hierarchy[i].replace('[]', '[0]')
                _list = re.findall('\[(.+?)\]', param)
                
                if _list:
                    _length = len(_list[0])
                    parameter += "[\'" + param[:-(_length + 2)] + "\']" + '[' + _list[0] + ']'
                else:
                    parameter += "[\'" + param + "\']"
        return parameter


    def getInstance(self, current_reference_uuid, instanceList):
        for instance in instanceList:
            if current_reference_uuid == instance.get('reference_uuid'):
                return instance
        return dict()


    def getIntent(self, current_reference_uuid, instanceList):
        for instance in instanceList:
            if instance.get('reference_uuid') == current_reference_uuid:
                return instance
        return dict()
    
    def getFlowIntent(self, instanceList):
        for instance in instanceList:
            if instance.get('reference_uuid') == 'flow_intent':
                return instance
        return dict()
                    

    def getValueFromIntent(self, name, body):
        property_name = '[\'' + name + '\']'
        try:
            value = eval('body' + property_name)
        except Exception:
            value = None
        return value
        
    
    def post(self):
        try:
            # old structer
            if self.flow_tracking.get('appInfo') is not None:
                return {}

            current_reference_uuid = self.flow_tracking.get('current_reference_uuid')
            instances = self.flow_tracking.get('instances') or list()

            self.logger.debug('GetcacheViews.post.current_reference_uuid: {}'.format(current_reference_uuid))
            self.logger.debug('GetcacheViews.post.instances: {}'.format(instances))

            instance = self.getInstance(current_reference_uuid, instances)
            sequence = instance.get('sequence') or 0
            member_instances = instance.get('member_instances') or list()
            intent_ins = list()

            self.logger.debug('GetcacheViews.post.instance: {}'.format(instance))
            self.logger.debug('GetcacheViews.post.sequence: {}'.format(sequence))
            self.logger.debug('GetcacheViews.post.member_instances: {}'.format(member_instances))

            if member_instances:
                instance_uuid = constants.INSTANCE_UUID
                for item in member_instances:
                    if instance_uuid == item.get('uuid'):
                        intent_ins = item.get('intent', dict()).get('ins') or list()
                        break
            else:
                intent_ins = instance.get('intent', dict()).get('ins') or list()

            result = dict()

            self.logger.debug('GetcacheViews.post.intent_ins: {}'.format(intent_ins))

            if intent_ins:
                for intent_in in intent_ins:
                    intent_in_name = intent_in.get('name')
                    if intent_in_name is None:
                        continue

                    intent_in_vtype = intent_in.get('vtype')
                    intent_in_type = intent_in.get('type')
                    intent_in_value = intent_in.get('value')
                    intent_in_multiple = intent_in.get('multiple') or False

                    field_value = None
                    object_multiple_constant = False

                    self.logger.debug('GetcacheViews.post.intent_in: {}'.format(intent_in))
                    self.logger.debug('GetcacheViews.post.intent_in_vtype: {}'.format(intent_in_vtype))

                    if intent_in_vtype == 'constant':
                        if intent_in_multiple == True:
                            if intent_in_type == 'object':
                                intent_in_value = '[' + intent_in_value + ']'
                                object_multiple_constant = True
                            else:
                                intent_in_value = intent_in_value.split(',')
                                intent_in_value = [item.strip() for item in intent_in_value]

                        field_value = intent_in_value

                    elif intent_in_vtype == 'system':
                        if intent_in_value == 'APP_CODE':
                            field_value = self.flow_tracking.get('login_info', {}).get('application_number') or self.flow_tracking.get('login_info', {}).get('app_id')
                        elif intent_in_value == 'BRANCH_INFO_ID':
                            field_value = self.flow_tracking.get('login_info', {}).get('user_branch_id') or self.flow_tracking.get('login_info', {}).get('branch')
                        elif intent_in_value == 'BRANCH_CODE':
                            field_value = self.flow_tracking.get('login_info', {}).get('user_branch_code')
                        elif intent_in_value == 'USER_ID':
                            field_value = self.flow_tracking.get('login_info', {}).get('user_id')
                        elif intent_in_value == 'USER_UCID':
                            field_value = self.flow_tracking.get('login_info', {}).get('user_ucid')
                        elif intent_in_value == 'EMPLOYEE_ID':
                            field_value = self.flow_tracking.get('login_info', {}).get('employee_id')
                        elif intent_in_value == 'USERNAME':
                            field_value = self.flow_tracking.get('login_info', {}).get('username')
                        elif intent_in_value == 'ERM_ROLE':
                            field_value = self.flow_tracking.get('login_info', {}).get('enterprise_role_code') or self.flow_tracking.get('login_info', {}).get('erm_role')
                        elif intent_in_value == 'ERM_ROLE_ID':
                            field_value = self.flow_tracking.get('login_info', {}).get('enterprise_role_id')
                        elif intent_in_value == 'COMPANY_INFOMATION':
                            field_value = self.flow_tracking.get('login_info', {}).get('company_ucid')
                        elif intent_in_value == 'COMPANY_CODE':
                            field_value = self.flow_tracking.get('login_info', {}).get('company_code')
                        elif intent_in_value == 'REGISTRATION_SERVICE_ID':
                            field_value = self.flow_tracking.get('login_info', {}).get('registration_service_id')
                        elif intent_in_value == 'ORGANIZATION_UNIT_ID':
                            field_value = self.flow_tracking.get('login_info', {}).get('user_organization_unit_id')
                        elif intent_in_value == 'ORGANIZATION_UNIT_CODE':
                            field_value = self.flow_tracking.get('login_info', {}).get('user_organization_unit_code')
                        elif intent_in_value == 'DATA_CONTROLLER':
                            field_value = self.flow_tracking.get('login_info', {}).get('data_controller')
                        elif intent_in_value == 'SUB_CONTROLLER':
                            field_value = self.flow_tracking.get('login_info', {}).get('sub_controller')
                        elif intent_in_value == 'DATA_PROCESSOR':
                            field_value = self.flow_tracking.get('login_info', {}).get('data_processor')
                        elif intent_in_value == 'FLOW_SESSION_ID':
                            field_value = self.flow_tracking.get('workflow_key')
                        elif intent_in_value == 'LOG_SESSION_ID':
                            field_value = self.flow_tracking.get('login_info', {}).get('log_session_id')
                            if field_value is None:
                                field_value = str(uuid.uuid4())
                            field_value = self.workflowkey
                        elif intent_in_value == 'CURRENT_DATE':
                            if intent_in_type in ['number', 'integer', 'decimal', 'datetime']:
                                current = datetime.now(pytz.timezone("Asia/Bangkok")) \
                                            .replace(hour=0, minute=0, second=0, microsecond=0) \
                                            .astimezone(pytz.utc)
                                field_value = round(current.timestamp()) * 1000
                                # field_value = int(round(time.time() * 1000))
                            elif intent_in_type == 'text':
                                field_value = datetime.now().strftime("%d/%m/%Y")
                        elif intent_in_value == 'CURRENT_TIME':
                            if intent_in_type in ['number', 'integer', 'decimal', 'datetime']:
                                field_value = int(round(time.time() * 1000))
                            elif intent_in_type == 'text':
                                field_value = datetime.now().strftime("%H:%M:%S")
                        elif intent_in_value == 'CURRENT_DATETIME':
                            if intent_in_type in ['number', 'integer', 'decimal', 'datetime']:
                                field_value = int(round(time.time() * 1000))
                            elif intent_in_type == 'text':
                                field_value = datetime.now().strftime("%d/%m/%Y %H:%M:%S")

                    elif intent_in_vtype == 'intent':
                        intent_in_reference_uuid = intent_in.get('reference_uuid')
                        intent_in_reference_instance = self.getInstance(intent_in_reference_uuid, instances)
                        vtype_intent_index = intent_in_reference_instance.get('sequence') or sequence - 1
                        vtype_intent_sub_index = intent_in_reference_instance.get('sub_sequence') or 0

                        self.logger.debug('GetcacheViews.post.intent.intent_in_reference_uuid: {}'.format(intent_in_reference_uuid))
                        self.logger.debug('GetcacheViews.post.intent.intent_in_reference_instance: {}'.format(intent_in_reference_instance))
                        self.logger.debug('GetcacheViews.post.intent.vtype_intent_index: {}'.format(vtype_intent_index))
                        self.logger.debug('GetcacheViews.post.intent.vtype_intent_sub_index: {}'.format(vtype_intent_sub_index))

                        key = 'INTENT_' + self.workflowkey
                        self.logger.debug('GetcacheViews.post.intent.key: {}'.format(key))
                    
                        try:
                            intent_cache = self.cache.get_json(key)
                        except Exception:
                            intent_cache = list()

                        self.logger.debug('GetcacheViews.post.intent.intent_cache: {}'.format(intent_cache))

                        if intent_cache:
                            intent_cache = intent_cache.get('intents')
                            self.logger.debug('GetcacheViews.post.intent.vtype_intent_index: {}'.format(vtype_intent_index))
                            self.logger.debug('GetcacheViews.post.intent.vtype_intent_sub_index: {}'.format(vtype_intent_sub_index))
                            self.logger.debug('GetcacheViews.post.intent.intent_cache: {}'.format(intent_cache))
                            instance = self.getIntent(intent_in_reference_uuid, intent_cache)
                            self.logger.debug('GetcacheViews.post.intent.instance: {}'.format(instance))
                            if instance:
                                self.logger.debug('GetcacheViews.post.intent.intent_in_value: {}'.format(intent_in_value))
                                _value = intent_in_value.replace('[]', '[0]')
                                self.logger.debug('GetcacheViews.post.intent._value: {}'.format(_value))
                                field_value = self.getValueFromIntent(_value, instance)
                                self.logger.debug('GetcacheViews.post.intent.field_value: {}'.format(field_value))

                    elif intent_in_vtype == 'flow_intent':
                        key = 'INTENT_' + self.workflowkey
                        self.logger.debug('GetcacheViews.post.flow_intent.key: {}'.format(key))

                        try:
                            intent_cache = self.cache.get_json(key)
                        except Exception:
                            intent_cache = list()

                        self.logger.debug('GetcacheViews.post.flow_intent.intent_cache: {}'.format(intent_cache))

                        if intent_cache:
                            intent_cache = intent_cache.get('intents')
                            self.logger.debug('GetcacheViews.post.flow_intent.intent_cache: {}'.format(intent_cache))
                            instance = self.getFlowIntent(intent_cache)
                            self.logger.debug('GetcacheViews.post.flow_intent.instance: {}'.format(instance))
                            if instance:
                                self.logger.debug('GetcacheViews.post.flow_intent.intent_in_value: {}'.format(intent_in_value))
                                _value = intent_in_value.replace('[]', '[0]')
                                self.logger.debug('GetcacheViews.post.flow_intent._value: {}'.format(_value))
                                field_value = self.getValueFromIntent(_value, instance)
                                self.logger.debug('GetcacheViews.post.flow_intent.field_value: {}'.format(field_value))

                    if field_value is not None:
                        try:
                            if intent_in_multiple == True:
                                result_list = list()
                                if isinstance(field_value, list):
                                    for item in field_value:
                                        item_value = self.convertTypeData(intent_in_type, item)
                                        result_list.append(item_value)
                                elif object_multiple_constant == True:
                                    field_value = self.convertTypeData(intent_in_type, field_value)
                                    result_list = field_value
                                else: 
                                    field_value = self.convertTypeData(intent_in_type, field_value)
                                    result_list.append(field_value)
                                        
                                field_value = result_list
                            else:
                                field_value = self.convertTypeDataForJson(intent_in_type, field_value)
                        except Exception as exc:
                            raise Exception(exc)
                    
                    if intent_in_type == 'datetime':
                        result[intent_in_name] = field_value
                    else:
                        intent_in_name = "[\'" + intent_in_name + "\']"
                        conmand = 'result' + intent_in_name + ' = {}'.format(field_value)
                        self.logger.debug('GetcacheViews.post.flow_intent.conmand: {}'.format(conmand))
                        exec(conmand)

                    self.logger.debug("[Get Cache] {}: set filed name {} to {}".format(self.workflowkey, intent_in, field_value))
            self.logger.debug('GetcacheViews.post.flow_intent.result: {}'.format(result))
            return result
        except Exception as exc:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            exception_message = exc_type, fname, exc_tb.tb_lineno
            self.logger.error('GetcacheViews.post.exc: {}'.format(exc))
            self.logger.error('GetcacheViews.post.exception_message: {}'.format(exception_message))
            raise Exception(exc)
