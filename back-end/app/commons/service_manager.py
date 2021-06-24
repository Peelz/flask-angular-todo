import sys
import os
import json
import time
import redis
import collections
import re
import magic

from datetime import datetime
from flask_api import status
from flask import current_app as app
from flask import request, jsonify

from app.commons import constants
from app.commons.adapters.additional_condition import AdditionalCondition
from app.commons.adapters.latest_update_date import LatestUpdateDate
from app.commons.adapters.data_privacy import DataPrivacy
from app.commons.adapters.search_transformation import SearchTransformation
from app.commons.adapters.create_transformation import CreateTransformation
from app.commons.adapters.update_transformation import UpdateTransformation
from app.commons.abstract_activity import AbstractActivity
from app.commons.connectors.launcher import Launcher
from app.commons.utils.tool import Tool

class ServiceManagerViews(AbstractActivity):

    def __init__(self):
        try:
            self.additional_condition = AdditionalCondition()
            self.latest_update_date = LatestUpdateDate()
            self.data_privacy = DataPrivacy()
            self.search_transformation = SearchTransformation()
            self.create_transformation = CreateTransformation()
            self.update_transformation = UpdateTransformation()
            super().__init__()
            self.logger.debug('ServiceManagerViews.__init__')
            self.logger.info('init call service manager')
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            exception_message = exc_type, fname, exc_tb.tb_lineno
            self.logger.error('ServiceManagerViews.__init__.e: {}'.format(e))
            self.logger.error('ServiceManagerViews.__init__.exception_message: {}'.format(exception_message))

    def process(self, request_data):
        try:
            store_response = False

            service_name = None
            body = None
            url_parameter = None
            stream = None
            group_key_fields = None

            if request_data.get('json') is not None or request_data.get('json'):
                service_name = request_data.get('json').get('service_name')
                body = request_data.get('json').get('body')
                url_parameter = request_data.get('json').get('url_parameter')
                if isinstance(body, list):
                    stream = body[0].get('stream')
                else:
                    stream = body.get('stream')
            elif request_data.get('form') is not None or request_data.get('form'):
                service_name = request_data.get('form').get('service_name')
                body = request_data.get('form').get('body')
                stream = json.loads(body).get('stream')
            
            self.logger.info('call service manager ({}) - service_name: {}'.format(service_name, service_name))
            self.logger.info('call service manager ({}) - body: {}'.format(service_name, body))
            self.logger.info('call service manager ({}) - url_parameter: {}'.format(service_name, url_parameter))
            self.logger.info('call service manager ({}) - stream: {}'.format(service_name, stream))
                
            if service_name is None:
                raise Exception('service_name is required')

            # adater for search
            service_adapter_search_list = ['data_query_normal_with_tql' \
                                            , 'data_query_journal_with_tql' \
                                            , 'data_query_normal' \
                                            , 'data_query_journal_with_cache' \
                                            , 'data_query_journal_with_latest_update_date']

            if service_name in service_adapter_search_list:

                self.logger.info('call service manager ({}) - found in search list'.format(service_name))

                filter_condition = ''
                filter_condition_order_by = ''
                if body and body.get('filter'):
                    filter_condition = body.get('filter') or ''
                    foundOrderBy = re.search(r'order\s*by', filter_condition, re.IGNORECASE)
                    if foundOrderBy:
                        filter_condition_order_by = filter_condition[foundOrderBy.start():]
                        filter_condition = filter_condition[0:foundOrderBy.start()]
                        body['filter'] = filter_condition
                
                if service_name == 'data_query_journal_with_cache':
                    dataset_name = body.get('dataset_name') or None
                    
                    if (dataset_name in getattr(constants, 'RELATED_PARAMETER_DATASETS', list()) or \
                        (getattr(constants, 'PARAMETER_DATASET', None) == True)):
                    
                        key = 'PARAMETER_{};{}'.format(dataset_name, filter_condition.replace(" ", ""))
                        response_cache = self.cache.get_json(key)

                        self.logger.debug('ServiceManagerViews.process.response_cache {}, {}, {}, {}'.format(dataset_name, key, response_cache, type(response_cache)))

                        if response_cache is not None and response_cache:
                            try:
                                response_cache = json.loads(response_cache)
                            except Exception as e:
                                exc_type, exc_obj, exc_tb = sys.exc_info()
                                fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                                exception_message = exc_type, fname, exc_tb.tb_lineno
                                self.logger.error('ServiceManagerViews.process.response_cache.e: {}'.format(e))
                                self.logger.error('ServiceManagerViews.process.response_cache.exception_message: {}'.format(exception_message))
                            response = self.ui_response.success('success', self.reference_id, response_cache)
                            
                            self.logger.info('call service manager ({}) - found in cache'.format(service_name))

                            return response
                    else:
                        body['options'] = dict()
                        body['options']['embed_public'] = False
                        body['options']['mapping'] = False
                        store_response = True
                
                if service_name == 'data_query_journal_with_latest_update_date':
                    group_key_fields = body.get('group_key_fields')
                    body = self.latest_update_date.adapter(body)

                # if getattr(constants, 'DATA_PRIVACY', False) == True:
                #     body = self.data_privacy.adapter(body)

                flag_data_privacy = getattr(constants, 'DATA_PRIVACY', False)
                self.logger.debug('ServiceManagerViews.post.data_privacy.constants.DATA_PRIVACY: {}'.format(flag_data_privacy))

                if body.get('isDisableDcDp') == True:
                    # support widget-marketing-license
                    body = self.get_wrapper_data_privacy_condition(body)
                else:
                    body = self.data_privacy.adapter(body)

                self.logger.debug('ServiceManagerViews.process.data_privacy.body: {}'.format(body))

                body = self.additional_condition.adapter(body)
                if filter_condition_order_by:
                    body['filter'] = '{} {}'.format(body['filter'], filter_condition_order_by)
                body = self.search_transformation.adapter(body)

            # adapter for create
            service_adapter_create_list = ['data_create_normal' \
                                            , 'data_create_journal']

            if service_name in service_adapter_create_list:

                self.logger.info('call service manager ({}) - found in create list'.format(service_name))

                dataset_name = body.get('dataset_name') or None
                body = self.create_transformation.adapter(body, self.logger)
                body['request_data'][0] = self.remove_not_in_meta_field(dataset_name, body['request_data'][0])


            # adapter for update
            service_adapter_update_list = ['data_update_normal' \
                                            , 'data_update_journal' \
                                            , 'data_update_master']
                                            
            if service_name in service_adapter_update_list:

                self.logger.info('call service manager ({}) - found in update list'.format(service_name))

                dataset_name = body.get('dataset_name') or None
                body = self.update_transformation.adapter(body)
                body['request_data'][0] = self.remove_not_in_meta_field(dataset_name, body['request_data'][0])

            body = self.get_login_info(body)
            url_parameter = self.get_login_info(url_parameter)

            self.logger.debug('ServiceManagerViews.process.service_name: {}'.format(service_name))
            self.logger.debug('ServiceManagerViews.process.body: {}'.format(body))
            self.logger.debug('ServiceManagerViews.process.url_parameter: {}'.format(url_parameter))
            self.logger.debug('ServiceManagerViews.process.stream: {}'.format(stream))

            self.logger.info('call service manager ({}) - service_name: {}'.format(service_name, service_name))
            self.logger.info('call service manager ({}) - body: {}'.format(service_name, body))
            self.logger.info('call service manager ({}) - url_parameter: {}'.format(service_name, url_parameter))
            self.logger.info('call service manager ({}) - stream: {}'.format(service_name, stream))

            if request.files:
                try:
                    document = request.files.get('file')
                    file_name = document.filename
                    file_suffix = None
                    if '.' in file_name:
                        file_suffix = file_name.split('.')[-1]
                        file_suffix = file_suffix.lower()
                    file_type = magic.from_buffer(document.read(), mime=True)
                    document.seek(0)
                    ms_types = [
                        'application/msword', 
                        'application/vnd.ms-excel', 
                        'application/vnd.openxmlformats-officedocument.wordprocessingml.document', 
                        'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', 
                        'application/zip'
                    ]
                    allowed_file_type = {
                        'jpg': ['image/jpeg'], 
                        'jpeg': ['image/jpeg'], 
                        'png': ['image/png'], 
                        'gif': ['image/gif'], 
                        'pdf': ['application/pdf'], 
                        'doc': ms_types, 
                        'xls': ms_types, 
                        'docx': ms_types, 
                        'xlsx': ms_types, 
                        'csv': ['text/plain'], 
                        'zip': ['application/zip'],
                    }
                    self.logger.info('call service manager ({}) - file_name: {}'.format(service_name, file_name))
                    self.logger.info('call service manager ({}) - file_type: {}'.format(service_name, file_type))
                    is_valid = False
                    if file_suffix in allowed_file_type.keys() and file_type in allowed_file_type[file_suffix]:
                        is_valid = True
                    if not is_valid:
                        raise Exception('file not allow')
                except Exception as e:
                    exc_type, exc_obj, exc_tb = sys.exc_info()
                    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                    exception_message = exc_type, fname, exc_tb.tb_lineno
                    self.logger.error('ServiceManagerViews.checkFileType.e: {}'.format(e))
                    self.logger.error('ServiceManagerViews.checkFileType.exception_message: {}'.format(exception_message))
                    raise Exception('file not allow')

            response_data = self.launcher_service.service_manager(service_name, body, url_parameter, request.files, stream)

            if(not isinstance(response_data, tuple)):
                response_data = response_data.json()

                if response_data.get('msg_code') == '30000':
                    if service_name == 'data_query_journal_with_latest_update_date':
                        if group_key_fields:
                            result_body = response_data.get('response_data') or dict()
                            
                            key = str()
                            for element in group_key_fields:
                                key += 'item[\'' + element + '\'], '
                            key = key[0:-2]

                            grouped = collections.defaultdict(list)
                            for item in result_body:
                                exec('grouped[' + key + '].append(item)')

                            result_body = list()
                            for model, group in grouped.items():
                                result_body.append(group[0])

                            response_data['response_data'] = result_body
                                
                        if store_response:
                            key = 'PARAMETER_{};{}'.format(dataset_name, filter_condition.replace(" ", "") or '')
                            self.cache.put_json(key, response_data, 24*60*60)

                response = self.ui_response.success('success', self.reference_id, response_data)
                return response
            else:
                return response_data

        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            exception_message = exc_type, fname, exc_tb.tb_lineno
            self.logger.error('ServiceManagerViews.process.e: {}'.format(e))
            self.logger.error('ServiceManagerViews.process.exception_message: {}'.format(exception_message))
            response = self.ui_response.error(str(e), self.reference_id)
            return response

    def post(self):

        self.logger.debug('ServiceManagerViews.post.start')

        self.logger.info('start call service manager')

        try:
            response = None
            if request.json is not None or request.json:
                request_data = {
                    'json': request.json
                }
                self.logger.info('call service manager - request_data (json): {}'.format(request_data))
                response = self.process(request_data)
            elif request.form is not None or request.form:
                request_data = {
                    'form': request.form
                }
                self.logger.info('call service manager - request_data (form)')
                response = self.process(request_data)
            
            self.logger.debug('ServiceManagerViews.post.response {}'.format(response))
            self.logger.debug('ServiceManagerViews.post.type(response) {}'.format(type(response)))

            self.logger.info('call service manager - response_type: {}'.format(type(response)))

            try:

                response_json_log = Tool.remove_sensitive_key_for_logging(response)

                self.logger.info('call service manager - response_json_log: {}'.format(response_json_log))

            except Exception as e:
                self.logger.error('ServiceManagerViews.post cannot .json()')

            check_if = not isinstance(response, tuple)

            self.logger.debug('ServiceManagerViews.post.check_if {}'.format(check_if))

            self.logger.info('end call service manager')

            if(not isinstance(response, tuple)):
                return jsonify(response)
            else:
                return response
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            exception_message = exc_type, fname, exc_tb.tb_lineno
            self.logger.error('ServiceManagerViews.post.e: {}'.format(e))
            self.logger.error('ServiceManagerViews.post.exception_message: {}'.format(exception_message))
            response = self.ui_response.error(str(e), self.reference_id)
            return jsonify(response)


    def get(self):
        try:
            service_name = request.args.get('service_name')
            if service_name is None:
                raise Exception('service_name is required')

            response_data = self.launcher_service.get_endpoind(service_name)
            response = {
                'endpoint': response_data['results'][0]['url']
            }

            response = self.ui_response.success('success', self.reference_id, response)
        
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            exception_message = exc_type, fname, exc_tb.tb_lineno
            self.logger.error('ServiceManagerViews.get.e: {}'.format(e))
            self.logger.error('ServiceManagerViews.get.exception_message: {}'.format(exception_message))
            response = self.ui_response.error(str(e), self.reference_id)

        finally:
            return jsonify(response), status.HTTP_200_OK
