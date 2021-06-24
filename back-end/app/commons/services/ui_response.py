from datetime import datetime
import sys
import traceback
import json

from app.commons import constants
from flask import session

class UIResponse:

    def __init__(self, logger):
        self.logger = logger


    def success(self, description, reference_id='', data=None):
        response = {
            "meta": {
                "response_code": "10000",
                "response_datetime": datetime.now().strftime("%d-%m-%Y %H:%M:%S"),
                "response_ref": reference_id,
                "response_desc": description
            }
        }

        if data is not None:
            response['data'] = data

        return response


    def error(self, description, reference_id=''):
        response = {
            "meta": {
                "response_code": "11000",
                "response_datetime": datetime.now().strftime("%d-%m-%Y %H:%M:%S"),
                "response_ref": reference_id,
                "response_desc": description
            }
        }

        return response


    def warning(self, description, reference_id='', data=None):
        response = {
            "meta": {
                "response_code": "12000",
                "response_datetime": datetime.now().strftime("%d-%m-%Y %H:%M:%S"),
                "response_ref": reference_id,
                "response_desc": description
            }
        }

        if data is not None:
            response['data'] = data
        return response


    def get_response_for_de(self, response_data, error_message=None):
        self.response_format = {
            "meta": {
                "response_ref": "",
                "response_datetime": datetime.now().strftime("%d-%m-%Y %H:%M:%S"),
                "response_code": "",
                "response_desc": ""
            },
            "data": {}
        }

        if isinstance(response_data, dict):
            response_data = response_data
        else:
            response_data = response_data.json()

        message_error = response_data.get('msg_code')
        message_error_detail = response_data.get('msg_detail')
        if message_error:
            response_msg = self._get_ui_message_by_dep_code(message_error)
            if not response_msg:
                response_msg = {
                    'code': response_data.get('msg_code'),
                    'msg_detail': response_data.get('msg_detail')
                }
            if message_error != '30000':
                response_msg = {
                    'code': response_msg.get('code'),
                    'msg_detail': message_error_detail
                }
            if response_data.get("response_data"):
                self.response_format['data']['response_data'] = response_data.get('response_data') or list()

            if response_data.get('hits'):
                self.response_format['data']['hits'] = response_data.get('hits')

            self.response_format['meta']['response_ref'] = session['reference_id']
            self.response_format['meta']['track_id'] = response_data.get('track_id')
            self.response_format['meta']['response_datetime'] = datetime.now().strftime("%d-%m-%Y %H:%M:%S")
            self.response_format['meta']['response_code'] = str(response_msg['code'])
            self.response_format['meta']['response_desc'] = response_msg['msg_detail']
        else:
            self.response_format['meta'] = response_data.get('meta')
        
        if 'error_index' in response_data and (response_data['error_index'] or response_data['error_index'] == 0):
            self.response_format['data']['error_index'] = response_data['error_index']
        if 'handle_error_indexs' in response_data and response_data['handle_error_indexs']:
            self.response_format['data']['handle_error_indexs'] = response_data['handle_error_indexs']

        return self.response_format


    def _get_ui_message_by_dep_code(self, dep_code):
        result = dict()

        dep_ui_messages = getattr(constants, 'DEP_UI_MESSAGE', list())
        for dep_ui_message in dep_ui_messages:
            if dep_ui_message.get('dep_code') == int(dep_code):
                result['code'] = dep_ui_message.get('ui_code') or dep_ui_message.get('dep_code')
                result['msg_detail'] = dep_ui_message.get('ui_dep_msg_detail') or dep_ui_message.get('dep_msg_detail')
                break

        return result
