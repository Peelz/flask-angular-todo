import sys
import os
import json
import requests
import uuid

from datetime import datetime
from flask import request, session
from flask import current_app as app

from app.commons.services.cache import Cache
from app.commons.login_data import LoginData
from app.commons.utils.tool import Tool
import time

from app.commons.jaeger_util import JaegerUtil

class Launcher():

    def __init__(self, logger, workflowkey=None):
        super().__init__()
        self.logger = logger
        self.logger.debug('Launcher.__init__')
        self.logger.info('init call launcher reverse')
        self.RESPONSE_SUCCESS_CODE = '10000'
        self.login_data = LoginData()
        if workflowkey:
            self.workflowkey = workflowkey
            self.flow_tracking = self.login_data.get_flow_tracking_with_wfk(self.workflowkey)
        else:
            self.workflowkey = self.login_data.get_work_flow_key()
            self.flow_tracking = self.login_data.get_flow_tracking()
        login_info = self.flow_tracking.get('login_info') or self.flow_tracking.get('appInfo')
        self.appid = int(login_info.get('app_id'))
        self.logger.debug('Launcher.__init__.self.workflowkey: {}'.format(self.workflowkey))
        self.logger.info('init call launcher reverse - workflowkey: {}'.format(self.workflowkey))

    def service_manager(self, service_name, parameter=dict(), url_parameter=dict(), request_files=None, stream=False):
        response = None
        try:

            self.logger.info('start call launcher reverse ({}) - service_name: {}'.format(service_name, service_name))
            self.logger.info('call launcher reverse ({}) - parameter: {}'.format(service_name, parameter))
            self.logger.info('call launcher reverse ({}) - url_parameter: {}'.format(service_name, url_parameter))
            self.logger.info('call launcher reverse ({}) - stream: {}'.format(service_name, stream))

            self.logger.debug('Launcher.service_manager.service_name: {}'.format(service_name))
            self.logger.debug('Launcher.service_manager.parameter: {}'.format(parameter))
            self.logger.debug('Launcher.service_manager.url_parameter: {}'.format(url_parameter))
            self.logger.debug('Launcher.service_manager.stream: {}'.format(stream))

            if service_name is None:
                raise Exception('service_name is require')

            if '_url' in parameter:
                url_parameter = parameter['_url']
                del parameter['_url']

            request_data = {
                '_data': parameter
            }

            if url_parameter:
                request_data["_url"] = url_parameter            

            endpoint, header = self._get_endpoint_and_header()
            endpoint = endpoint + service_name

            try:
                ignore_append_meta_app_no = ['user_defined_execution', 'execution_service']
                if service_name in ignore_append_meta_app_no:
                    app_meta = header.get('app-meta', None)
                    if app_meta:
                        app_meta = json.loads(app_meta)
                        if 'app_no' in app_meta:
                            del app_meta['app_no']
                            header['app-meta'] = json.dumps(app_meta)
            except Exception as e:
                exc_type, exc_obj, exc_tb = sys.exc_info()
                fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                exception_message = exc_type, fname, exc_tb.tb_lineno
                self.logger.error('Launcher.ignore_append_meta_app_no.e: {}'.format(e))
                self.logger.error('Launcher.ignore_append_meta_app_no.exception_message: {}'.format(exception_message))

            if request_files:
                document = request_files.get('file')
                file_name = document.filename
                file_type = document.content_type

                files = {
                    'file': (file_name, document.read(), file_type)
                }
                header.pop('content-Type')

                header = JaegerUtil.inject_header(None, endpoint, 'POST', header)

                self.logger.debug('Launcher.service_manager.endpoint: {}'.format(endpoint))
                self.logger.debug('Launcher.service_manager.header: {}'.format(header))
                self.logger.debug('Launcher.service_manager.request_data: {}'.format(request_data))
                self.logger.debug('Launcher.service_manager.file_name: {}'.format(file_name))
                self.logger.debug('Launcher.service_manager.file_type: {}'.format(file_type))
                self.logger.debug('Launcher.service_manager.stream: {}'.format(stream))

                self.logger.info('call launcher reverse ({}) - endpoint: {}'.format(service_name, endpoint))
                header_log = Tool.remove_sensitive_key_for_logging(header)
                self.logger.info('call launcher reverse ({}) - header_log: {}'.format(service_name, header_log))
                self.logger.info('call launcher reverse ({}) - request_data: {}'.format(service_name, request_data))
                self.logger.info('call launcher reverse ({}) - file_name: {}'.format(service_name, file_name))
                self.logger.info('call launcher reverse ({}) - file_type: {}'.format(service_name, file_type))
                self.logger.info('call launcher reverse ({}) - stream: {}'.format(service_name, stream))

                start_time = time.time()
                
                if JaegerUtil.tracer:
                    with JaegerUtil.tracer.start_active_span(endpoint) as scope:
                        if scope and scope.span:
                            scope.span.log_kv({'event': 'launcher'})
                            JaegerUtil.active_span = scope.span
                            response = requests.post(endpoint, headers=header, data=json.loads(request_data['_data']), verify=None, files=files, stream=stream)
                
                self.logger.debug('Launcher.service_manager.response.text: {}'.format(response.text))
                end_time = time.time()
                used_time = (end_time - start_time) * 1000

                self.logger.debug('Launcher.service_manager.used_time: {}'.format(used_time))
                self.logger.debug('Launcher.service_manager.response: {}'.format(response))

                self.logger.info('call launcher reverse ({}) - used_time: {}'.format(service_name, used_time))
                self.logger.info('call launcher reverse ({}) - response: {}'.format(service_name, response))

                try:

                    self.logger.debug('Launcher.service_manager.response.json(): {}'.format(response.json()))

                    response_json = response.json()
                    response_json_log = Tool.remove_sensitive_key_for_logging(response_json)

                    self.logger.info('call launcher reverse ({}) - response_json_log: {}'.format(service_name, response_json_log))

                except Exception as e:
                    self.logger.error('Launcher.service_manager cannot .json()')
            else:

                header = JaegerUtil.inject_header(None, endpoint, 'POST', header)

                self.logger.debug('Launcher.service_manager.endpoint: {}'.format(endpoint))
                self.logger.debug('Launcher.service_manager.header: {}'.format(header))
                self.logger.debug('Launcher.service_manager.request_data: {}'.format(request_data))
                self.logger.debug('Launcher.service_manager.stream: {}'.format(stream))

                self.logger.info('call launcher reverse ({}) - endpoint: {}'.format(service_name, endpoint))
                header_log = Tool.remove_sensitive_key_for_logging(header)
                self.logger.info('call launcher reverse ({}) - header_log: {}'.format(service_name, header_log))
                self.logger.info('call launcher reverse ({}) - request_data: {}'.format(service_name, request_data))
                self.logger.info('call launcher reverse ({}) - stream: {}'.format(service_name, stream))

                start_time = time.time()

                if JaegerUtil.tracer:
                    with JaegerUtil.tracer.start_active_span(endpoint) as scope:
                        if scope and scope.span:
                            scope.span.log_kv({'event': 'launcher'})
                            JaegerUtil.active_span = scope.span
                            response = requests.post(endpoint, headers=header, data=json.dumps(request_data), stream=stream)

                if not stream:
                    self.logger.debug('Launcher.service_manager.response.text: {}'.format(response.text))
                    if response.status_code != 200:
                        self.logger.error('Launcher.service_manager.error.response.text: {}'.format(response.text))
                end_time = time.time()
                used_time = (end_time - start_time) * 1000

                self.logger.debug('Launcher.service_manager.used_time: {}'.format(used_time))

                self.logger.info('call launcher reverse - used_time: {}'.format(used_time))

                # response.raise_for_status()
                if stream == True:
                    response_header = dict(response.headers)
                    header = {
                        'access-control-expose-headers': response_header.get('access-control-expose-headers'),
                        'access-control-max-age': response_header.get('access-control-max-age'),
                        'content-disposition': response_header.get('content-disposition'),
                        'content-type': response_header.get('Content-Type'),
                        'Content-Length': response_header.get('Content-Length'),
                    }
                    return response.raw.read(), header
                else:

                    self.logger.debug('Launcher.service_manager.response: {}'.format(response))

                    self.logger.info('call launcher reverse ({}) - response: {}'.format(service_name, response))

                    response_json = {}

                    try:

                        self.logger.debug('Launcher.service_manager.response.json(): {}'.format(response.json()))

                        response_json = response.json()
                        response_json_log = Tool.remove_sensitive_key_for_logging(response_json)

                        self.logger.info('call launcher reverse ({}) - response_json_log: {}'.format(service_name, response_json_log))

                    except Exception as e:
                        self.logger.error('Launcher.service_manager cannot .json()')
                    
                    if 'msg_code' in response_json and response_json.get('msg_code') != '30000':
                        self.logger.info('call launcher reverse ({}) - response (msg_code != 30000): {}'.format(service_name, response_json))

                    if response.status_code != 200:
                        response_desc = None
                        try:
                            response_json = response.json()
                            response_desc = response_json.get('response_desc')
                        except Exception as e:
                            response_desc = response.status_code
                        raise Exception('{}'.format(response_desc))

            if response.status_code != 200:
                raise Exception(
                    'response.status_code: {}'.format(response.status_code))
                
            self.logger.info('end call launcher reverse ({}) - service_name: {}'.format(service_name, service_name))

            return response
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            exception_message = exc_type, fname, exc_tb.tb_lineno
            self.logger.error('Launcher.service_manager.e: {}'.format(e))
            self.logger.error('Launcher.service_manager.exception_message: {}'.format(exception_message))
            raise e

    def _get_endpoint_and_header(self):
        try:
            endpoint = app.config.get('LAUNCHER_ENDPOINT')
            mule_client_id = app.config.get('MULE_CLIENT_ID')
            mule_client_secret = app.config.get('MULE_CLIENT_SECRET')

            self.logger.debug('Launcher._get_endpoint_and_header.self.workflowkey: {}'.format(self.workflowkey))

            flow_tracking = self.login_data.get_flow_tracking_with_wfk(self.workflowkey)
            
            login_info = flow_tracking.get('login_info')
            user_id = login_info.get('user_id')
            username = login_info.get('username')
            log_session_id = login_info.get('log_session_id') or str(uuid.uuid4())

            app_meta = {
                'user_id': user_id,
                'user_name': username,
                'state': app.config.get('CURRENT_STATE'),
                'sub_state': app.config.get('CURRENT_SUB_STATE'),
                'request_datetime': datetime.now().strftime("%d-%m-%Y %H:%M:%S"),
                'log_session_id': session.get('reference_id'),
                'app_no': str(self.appid),
                'flow_session_id': self.workflowkey,
            }

            # app_meta = {
            #     'user_id': '9f526af6-a254-4588-8d4f-739ec5b26fde',
            #     'user_name': 'user_nodc@tisco.co.th',
            #     'state': app.config.get('CURRENT_STATE'),
            #     'sub_state': app.config.get('CURRENT_SUB_STATE'),
            #     'request_datetime': datetime.now().strftime("%d-%m-%Y %H:%M:%S"),
            #     'log_session_id': 'XwVJn3mMkFzni@@AjLz@VgAADwc',
            #     'action_id': self.workflowkey,
            #     'ermrole': '69e85559-e9eb-49b5-9dd9-efc9de87af3d',
            #     'appid': '88',
            # }
            # for test c5 local
            
            return endpoint, {
                'content-Type': 'application/json',
                'app-meta': json.dumps(app_meta),
                'client_id': mule_client_id,
                'client_secret': mule_client_secret,
            }

        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            exception_message = exc_type, fname, exc_tb.tb_lineno
            self.logger.error('Launcher._get_endpoint_and_header.e: {}'.format(e))
            self.logger.error('Launcher._get_endpoint_and_header.exception_message: {}'.format(exception_message))
            raise e
