import sys
import os
import json
import re
import traceback
import uuid

from datetime import datetime

from flask import request, jsonify
from flask import current_app as app
from flask_api import status

from app.commons.services.cache import Cache
from app.commons.services.ui_response import UIResponse
from app.commons import constants


class AbstractFlowTrackingInformation:

    def __init__(self):
        self.workflowkey = request.headers.get('wfk')

        flow_tracking_key = 'FLOWTRACKING_{}'.format(self.workflowkey)
        self.flow_tracking = Cache().get_json(flow_tracking_key)

        if app.config.get('LOCAL') or False:
            # mock
            self.workflowkey = '1245e698-c64b-467e-8426-41e5828fe083'
            self.flow_tracking = {
                'code': 'Shelf_I9_D10_419',
                'current_node_sequence': 1,
                'current_instance_sequence': 1,
                'nodes': [{
                    'sequence': 1,
                    'instances': [{
                        'sequence': 1,
                        'uuid': '1245e698-c64b-467e-8426-41e5828fe083',
                    }]
                }],
                'login_info': {
                    'app_id': 65,
                    'user_id': '00u6ilmd4qVBUfkMC0h7',
                    'username': 'channel service account',
                    'role': '65-1',
                    'erm_role': 'a9858b85-d486-45bb-bf83-30d0bd2172ac',
                    'firstname': 'Andromeda',
                    'lastname': 'Shun',
                    'env': '',
                }
            }

        self.flow_code = self.flow_tracking.get('code')
        login_info = self.flow_tracking.get('login_info')
        self.app_code = login_info.get('app_id')
        self.app_name = login_info.get('applabel')
        self.user_id = login_info.get('user_id')
        self.username = login_info.get('username')
        self.log_session_id = login_info.get('log_session_id') or str(uuid.uuid4())
        self.role = login_info.get('role')
        self.ermrole = login_info.get('erm_role')
        self.firstname = login_info.get('firstname')
        self.lastname = login_info.get('lastname')
        self.env = login_info.get('env')
        self.representative_app = self.flow_tracking.get('representative_app') or False
        self.override_data_controller = self.flow_tracking.get('override_data_controller') or False 
        self.all_data_processor = self.flow_tracking.get('all_data_processor') or False

        self.branch_information  = login_info.get('branch')
        self.user_ucid = login_info.get('user_ucid')
        self.sub_controller = login_info.get('sub_controller')
        self.data_controller = login_info.get('data_controller')
        self.data_processor = login_info.get('data_processor')

        register_information = self.flow_tracking.get('register_info') or dict()
        register_application = register_information.get('register_application')[0] if type(register_information.get('register_application')) is list else dict()
        self.execution_application = register_application.get('application_code')

        # override data controller
        if self.representative_app:
            if self.override_data_controller:
                self.data_controller = self.data_processor

        current_node_sequence = self.flow_tracking.get('current_node_sequence')
        current_instance_sequence = self.flow_tracking.get('current_instance_sequence')

        nodes = self.flow_tracking.get('nodes') or list()
        self.current_instance = dict()
        for node in nodes:
            if node.get('sequence') == current_node_sequence:
                instances = node.get('instances') or list()
                for instance in instances:
                    if instance.get('sequence') == current_instance_sequence:
                        self.current_instance = instance
                        break
                break

    def get_login_info(self,body):
        temp_body = json.dumps(body)
        parameters = []
        parameter_all = re.findall('{{(.+?)}}',temp_body)
        for item in parameter_all:
            if item not in parameters:
                parameters.append(item)
        if parameters:
            for param in parameters:
                value = self.flow_tracking.get('login_info').get(param, '')
                temp_body = temp_body.replace('{{' + param + '}}', value)
        return json.loads(temp_body)
