import sys
import os
import json
import re
import traceback
import uuid
import pytz
import time

from datetime import datetime

from flask import request, jsonify, session
from flask import current_app as app
from flask_api import status

from app.commons.services.cache import Cache
from app.commons.services.ui_response import UIResponse
from app.commons import constants
from app.commons.services.log import Logger
from app.commons.connectors.launcher import Launcher
from app.commons.getcache import GetcacheViews
from app.commons.login_data import LoginData

class AbstractFlowTrackingInformation:

    def __init__(self):
        try:
            self.reference_id = session['reference_id']
            self.logger = Logger()

            self.login_data = LoginData()
            
            self.workflowkey = self.login_data.get_work_flow_key()
            self.flow_tracking = self.login_data.get_flow_tracking()

            self.logger.debug('AbstractFlowTrackingInformation.__init__.self.workflowkey 3: {}'.format(self.workflowkey))
            self.logger.debug('AbstractFlowTrackingInformation.__init__.LOCAL: {}'.format(app.config.get('LOCAL')))

            self.flow_code = self.flow_tracking.get('code')
            self.flow_intents = self.flow_tracking.get('flow_intents') or list()
            login_info = self.flow_tracking.get('login_info')
            self.flow_uuid = self.flow_tracking.get('uuid')
            self.app_code = int(login_info.get('app_id'))
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
            self.all_data_controller = self.flow_tracking.get('all_data_controller') or False

            self.branch_information  = login_info.get('user_branch_id') or login_info.get('branch')
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

            self.current_instance = self.get_current_instance()

            self.getcache_views = GetcacheViews()
            self.cache = Cache()

        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            exception_message = exc_type, fname, exc_tb.tb_lineno
            self.logger.error('AbstractFlowTrackingInformation.__init__.e: {}'.format(e))
            self.logger.error('AbstractFlowTrackingInformation.__init__.exception_message: {}'.format(exception_message))

    def get_current_instance(self):
        current_instance = dict()
        current_reference_uuid = self.flow_tracking.get('current_reference_uuid')
        instances = self.flow_tracking.get('instances') or list()
        for instance in instances:
            if current_reference_uuid == instance.get('reference_uuid'):
                current_instance = instance
        return current_instance
    
    def get_login_info(self,body):
        temp_body = json.dumps(body)
        parameters = []
        parameter_all = re.findall('{{(.+?)}}',temp_body)
        parseIntParameter = ["app_id"]
        for item in parameter_all:
            if item not in parameters:
                parameters.append(item)
        if parameters:
            for param in parameters:
                value = self.flow_tracking.get('login_info').get(param, None)
                replaceStr = '{{' + param + '}}'
                idx_rpIdx = temp_body.find(replaceStr)
                if(param in parseIntParameter):
                    replaceStr = '\"{{' + param + '}}\"'
                if value == None:
                    if temp_body[idx_rpIdx - 1] == '"':
                        replaceStr = '\"{{' + param + '}}\"'
                    if temp_body[idx_rpIdx - 1] == '"' and temp_body[idx_rpIdx - 2] == '\\':
                        replaceStr = '\\\"{{' + param + '}}\\\"'
                    temp_body = temp_body.replace(replaceStr, str("null"))
                else:
                    temp_body = temp_body.replace(replaceStr, str(value))
        return json.loads(temp_body)

    def find_system_value(self, parameter):
        if parameter == 'APP_CODE':
            return self.flow_tracking.get('login_info', {}).get('application_number') or self.flow_tracking.get('login_info', {}).get('app_id')
        elif parameter == 'BRANCH_INFO_ID':
            return self.flow_tracking.get('login_info', {}).get('user_branch_id') or self.flow_tracking.get('login_info', {}).get('branch')
        elif parameter == 'BRANCH_CODE':
            return self.flow_tracking.get('login_info', {}).get('user_branch_code')
        elif parameter == 'USER_ID':
            return self.flow_tracking.get('login_info', {}).get('user_id')
        elif parameter == 'USER_UCID':
            return self.flow_tracking.get('login_info', {}).get('user_ucid')
        elif parameter == 'EMPLOYEE_ID':
            return self.flow_tracking.get('login_info', {}).get('employee_id')
        elif parameter == 'USERNAME':
            return self.flow_tracking.get('login_info', {}).get('username')
        elif parameter == 'ERM_ROLE':
            return self.flow_tracking.get('login_info', {}).get('enterprise_role_code') or self.flow_tracking.get('login_info', {}).get('erm_role')
        elif parameter == 'ERM_ROLE_ID':
            return self.flow_tracking.get('login_info', {}).get('enterprise_role_id')
        elif parameter == 'COMPANY_INFOMATION':
            return self.flow_tracking.get('login_info', {}).get('company_ucid')
        elif parameter == 'COMPANY_CODE':
            return self.flow_tracking.get('login_info', {}).get('company_code')
        elif parameter == 'REGISTRATION_SERVICE_ID':
            return self.flow_tracking.get('login_info', {}).get('registration_service_id')
        elif parameter == 'ORGANIZATION_UNIT_ID':
            return self.flow_tracking.get('login_info', {}).get('user_organization_unit_id')
        elif parameter == 'ORGANIZATION_UNIT_CODE':
            return self.flow_tracking.get('login_info', {}).get('user_organization_unit_code')
        elif parameter == 'DATA_CONTROLLER':
            return self.flow_tracking.get('login_info', {}).get('data_controller')
        elif parameter == 'SUB_CONTROLLER':
            return self.flow_tracking.get('login_info', {}).get('sub_controller')
        elif parameter == 'DATA_PROCESSOR':
            return self.flow_tracking.get('login_info', {}).get('data_processor')
        elif parameter == 'FLOW_SESSION_ID':
            return self.flow_tracking.get('workflow_key')
        elif parameter == 'LOG_SESSION_ID':
            field_value = self.flow_tracking.get('login_info', {}).get('log_session_id')
            if field_value is None:
                field_value = str(uuid.uuid4())
            field_value = self.workflowkey
            return field_value
        elif parameter == 'CURRENT_DATE':
            current_date = datetime.now(pytz.timezone("Asia/Bangkok")) \
                .replace(hour=0, minute=0, second=0, microsecond=0) \
                .astimezone(pytz.utc)
            return int(round(current_date.timestamp()) * 1000)
        elif parameter == 'CURRENT_TIME':
            return int(round(time.time() * 1000))
        elif parameter == 'CURRENT_DATETIME':
            return int(round(time.time() * 1000))

    def cast_dynamic_additional(self, condition, current_cache, flag_condition = True):
        core_condition = condition
        try:
            current_node_cache = current_cache

            format_system_parameters = re.findall(r'\${(.*)}', core_condition)
            format_floworself_parameters = re.findall(r'\$\w+[\.\w]+', core_condition)

            format_system_parameters = sorted(format_system_parameters, key=len, reverse=True)
            format_floworself_parameters = sorted(format_floworself_parameters, key=len, reverse=True)

            self.logger.debug('AbstractFlowTrackingInformation.cast_dynamic_additional.condition: {}'.format(condition))
            self.logger.debug('AbstractFlowTrackingInformation.cast_dynamic_additional.format_system_parameters: {}'.format(format_system_parameters))
            self.logger.debug('AbstractFlowTrackingInformation.cast_dynamic_additional.format_floworself_parameters: {}'.format(format_floworself_parameters))

            for format_parameter in format_system_parameters:
                parameter_value = self.find_system_value(format_parameter)
                
                if parameter_value is not None:
                    if flag_condition:
                        if isinstance(parameter_value, str):
                            core_condition = core_condition.replace('${' + format_parameter + '}', str('"' + parameter_value + '"'))
                        elif isinstance(parameter_value, int):
                            core_condition = core_condition.replace('${' + format_parameter + '}', str(parameter_value))
                        elif isinstance(parameter_value, bool):
                            core_condition = core_condition.replace('${' + format_parameter + '}', str(parameter_value))
                    else:
                        core_condition = core_condition.replace('${' + format_parameter + '}', str(parameter_value))
                else:
                    core_condition = core_condition.replace('${' + format_parameter + '}', str("null"))
                    

            flow_intent = dict()

            if format_floworself_parameters:
                key = 'INTENT_' + self.workflowkey
                self.logger.debug('AbstractFlowTrackingInformation.cast_dynamic_additional.flow_intent.key: {}'.format(key))

                try:
                    intent_cache = Cache().get_json(key)
                except Exception:
                    intent_cache = list()
                
                self.logger.debug('AbstractFlowTrackingInformation.cast_dynamic_additional.intent_cache: {}'.format(intent_cache))
                intents = intent_cache['intents']
                flow_intent = next((item for item in intents if item["reference_uuid"] == "flow_intent"), dict())
                self.logger.debug('AbstractFlowTrackingInformation.cast_dynamic_additional.flow_intent: {}'.format(flow_intent))

            for format_parameter in format_floworself_parameters:
                # cut_key_value = format_parameter.split('.')
                # flag_intent, key_intent = cut_key_value[0], cut_key_value[1]
                idx_cut = format_parameter.find(".")
                flag_intent = format_parameter[:idx_cut]
                key_intent = format_parameter[idx_cut+1:]
                value_intent = None
                if flag_intent[1:] == 'Flow' and flow_intent:
                    value_intent = flow_intent.get(key_intent, None)
                elif flag_intent[1:] == 'Self' and current_node_cache:
                    value_intent = current_node_cache.get(key_intent, None)
                self.logger.debug('AbstractFlowTrackingInformation.cast_dynamic_additional.value_intent: {}'.format(value_intent))

                if value_intent is not None:
                    if flag_condition:
                        if isinstance(value_intent, str):
                            core_condition = core_condition.replace(format_parameter, str('"' + value_intent + '"'))
                        elif isinstance(value_intent, list):
                            if isinstance(value_intent[0], str):
                                core_condition = core_condition.replace(format_parameter, str(','.join(list(map(lambda x: '"' + x + '"', value_intent)))))
                            else:
                                core_condition = core_condition.replace(format_parameter, str(','.join(map(str, value_intent))))
                        elif isinstance(value_intent, int):
                            core_condition = core_condition.replace(format_parameter, str(value_intent))
                        elif isinstance(value_intent, bool):
                            core_condition = core_condition.replace(format_parameter, str(value_intent))
                    else:
                        core_condition = core_condition.replace(format_parameter, str(value_intent))
                else:
                    core_condition = core_condition.replace(format_parameter, str("null"))
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            exception_message = exc_type, fname, exc_tb.tb_lineno
            self.logger.error('AbstractFlowTrackingInformation.cast_dynamic_additional.e: {}'.format(e))
            self.logger.error('AbstractFlowTrackingInformation.cast_dynamic_additional.exception_message: {}'.format(exception_message))
        return core_condition
    
    def getMetaData(self, datasetName, descriptionDataset=None): 
        return self.getMetaData_2(Launcher(self.logger, self.workflowkey), datasetName, descriptionDataset)
    
    def getMetaData_2(self, launcher, datasetName, descriptionDataset=None): 
        fieldList = None
        # urls = "https://api-v1-service.c1-alpha-tiscogroup.com/public/data-v2-service/dep-api/GetMetaData?client_id=7d54f79fb17946678775205aab308619&client_secret=530d2e1111174443BFB01891E40BD091"
        if(datasetName):
            data = {"dataset_name": datasetName}
            resp = launcher.service_manager('data_get_metadata', data, None)
            if resp.ok :
                resp = resp.json()
                # response = requests.post(urls,data=json.dumps(data),headers=self.pHeaders)
                # resp = response.json()
                if(resp["msg_code"] == "30000"):
                    if(descriptionDataset):
                        if(descriptionDataset == resp["response_data"]["description"]):
                            fieldList = resp["response_data"]["fields"]
                            # self.logger.debug(os.path.basename(__file__), log.lineno(),
                            #             "{} Dataset :: {} field".format(datasetName, len(fieldList)))
                    else:
                        fieldList = resp["response_data"]["fields"]
                        # self.logger.debug(os.path.basename(__file__), log.lineno(),
                        #             "{} Dataset :: {} field".format(datasetName, len(fieldList)))
                elif resp["msg_code"] == "32902":
                    self.logger.error('call getMetaData ({}) - 32902: {}'.format(datasetName, resp.get('msg_detail')))
                    fieldList = self.get_metadata_service_account_role(launcher, datasetName, descriptionDataset)
                else:
                    self.logger.error('call getMetaData ({}) - error: {}'.format(datasetName, resp))
                    raise Exception('getMetaData - {} - {}'.format(resp.get('msg_code'), resp.get('msg_detail')))
            else:
                self.logger.error('call getMetaData ({}) - call fail'.format(datasetName))
                raise Exception('getMetaData - call fail')
        return fieldList
        
    def get_metadata_service_account_role(self, launcher, datasetName, descriptionDataset=None): 
        fieldList = None
        # urls = "https://api-v1-service.c1-alpha-tiscogroup.com/public/data-v2-service/dep-api/GetMetaData?client_id=7d54f79fb17946678775205aab308619&client_secret=530d2e1111174443BFB01891E40BD091"
        if(datasetName):
            data = {"dataset_name": datasetName}
            resp = launcher.service_manager('data_get_metadata_service_account_role', data, None)
            if resp.ok :
                resp = resp.json()
                # response = requests.post(urls,data=json.dumps(data),headers=self.pHeaders)
                # resp = response.json()
                if(resp["msg_code"] == "30000"):
                    if(descriptionDataset):
                        if(descriptionDataset == resp["response_data"]["description"]):
                            fieldList = resp["response_data"]["fields"]
                            # self.logger.debug(os.path.basename(__file__), log.lineno(),
                            #             "{} Dataset :: {} field".format(datasetName, len(fieldList)))
                    else:
                        fieldList = resp["response_data"]["fields"]
                        # self.logger.debug(os.path.basename(__file__), log.lineno(),
                        #             "{} Dataset :: {} field".format(datasetName, len(fieldList)))
                elif resp["msg_code"] == "32902":
                    self.logger.error('call get_metadata_service_account_role ({}) - error: {}'.format(datasetName, resp))
                    raise Exception('Get metadata - {} Not authorized'.format(datasetName))
                else:
                    self.logger.error('call get_metadata_service_account_role ({}) - error: {}'.format(datasetName, resp))
                    raise Exception('get_metadata_service_account_role - {} - {}'.format(resp.get('msg_code'), resp.get('msg_detail')))
            else:
                self.logger.error('call get_metadata_service_account_role ({}) - call fail'.format(datasetName))
                raise Exception('get_metadata_service_account_role - call fail')
        return fieldList
    
    def remove_not_in_meta_field(self, datasetName, data_field):
        focus_fields = [constants.FIELD_DATA_CONTROLLER, constants.FIELD_DATA_PROCESSOR, constants.FIELD_EXECUTION_APPLICATION, constants.FIELD_EXECUTION_LOCATION, constants.FIELD_THIRD_PARTY, constants.FIELD_SUB_CONTROLLER]
        remove_fields = []
        if datasetName and data_field:
            meta_fields = self.getMetaData(datasetName)
            for focus_field in focus_fields:
                is_found = False
                for meta_field in meta_fields:
                    if meta_field["name"] == focus_field:
                        is_found = True
                if not is_found:
                    remove_fields.append(focus_field)
            for remove_field in remove_fields:
                if remove_field in data_field:
                    data_field.pop(remove_field)
        return data_field
    
    def data_privacy_condition(self, dataset_name, condition):
        return self.data_privacy_condition_2(Launcher(self.logger, self.workflowkey), dataset_name, condition, False)

    def data_privacy_condition_2(self, launcher, dataset_name, condition, is_alway_check_data_privacy):
        try:
            is_found_dp_field = False
            is_found_dc_field = False
            is_found_sub_field = False
            self.logger.debug('AbstractFlowTrackingInformation.data_privacy_condition_2.dataset_name ({}): {}'.format(dataset_name, dataset_name))
            self.logger.debug('AbstractFlowTrackingInformation.data_privacy_condition_2.condition ({}): {}'.format(dataset_name, condition))
            self.logger.debug('AbstractFlowTrackingInformation.data_privacy_condition_2.is_alway_check_data_privacy ({}): {}'.format(dataset_name, is_alway_check_data_privacy))

            self.logger.info('call concast data privacy ({}) - condition: {}'.format(dataset_name, condition))
            self.logger.info('call concast data privacy ({}) - is_alway_check_data_privacy: {}'.format(dataset_name, is_alway_check_data_privacy))

            meta_fields = self.getMetaData_2(launcher, dataset_name)
            self.logger.debug('AbstractFlowTrackingInformation.data_privacy_condition_2.meta_fields ({}): {}'.format(dataset_name, meta_fields))

            self.logger.info('call concast data privacy ({}) - meta_fields: {}'.format(dataset_name, meta_fields))

            for meta_field in meta_fields:
                if meta_field["name"] == constants.FIELD_DATA_PROCESSOR:
                    is_found_dp_field = True
                elif meta_field["name"] == constants.FIELD_DATA_CONTROLLER:
                    is_found_dc_field = True
                elif meta_field["name"] == constants.FIELD_SUB_CONTROLLER:
                    is_found_sub_field = True
            # is_data_privacy = getattr(constants, 'DATA_PRIVACY', False)
            # if is_alway_check_data_privacy:
            #     is_data_privacy = True
            is_data_privacy = True
            self.logger.debug('AbstractFlowTrackingInformation.data_privacy_condition_2.is_found_dp_field ({}): {}'.format(dataset_name, is_found_dp_field))
            self.logger.debug('AbstractFlowTrackingInformation.data_privacy_condition_2.is_found_dc_field ({}): {}'.format(dataset_name, is_found_dc_field))
            self.logger.debug('AbstractFlowTrackingInformation.data_privacy_condition_2.is_found_sub_field ({}): {}'.format(dataset_name, is_found_sub_field))
            self.logger.debug('AbstractFlowTrackingInformation.data_privacy_condition_2.is_data_privacy ({}): {}'.format(dataset_name, is_data_privacy))

            self.logger.info('call concast data privacy ({}) - is_data_privacy: {}'.format(dataset_name, is_data_privacy))
            self.logger.info('call concast data privacy ({}) - is_found_dp_field: {}'.format(dataset_name, is_found_dp_field))
            self.logger.info('call concast data privacy ({}) - is_found_dc_field: {}'.format(dataset_name, is_found_dc_field))
            self.logger.info('call concast data privacy ({}) - is_found_sub_field: {}'.format(dataset_name, is_found_sub_field))

            if is_data_privacy and is_found_dp_field and is_found_dc_field and is_found_sub_field:
                overwrite_dp = None
                overwrite_dc = None
                overwrite_sub = None
                current_node_cache = self.getcache_views.post()
                self.logger.debug('AbstractFlowTrackingInformation.data_privacy_condition_2.current_node_cache ({}): {}'.format(dataset_name, current_node_cache))
                if constants.FIELD_DATA_PROCESSOR in current_node_cache and current_node_cache[constants.FIELD_DATA_PROCESSOR]:
                    overwrite_dp = current_node_cache[constants.FIELD_DATA_PROCESSOR]
                if constants.FIELD_DATA_CONTROLLER in current_node_cache and current_node_cache[constants.FIELD_DATA_CONTROLLER]:
                    overwrite_dc = current_node_cache[constants.FIELD_DATA_CONTROLLER]
                if constants.FIELD_SUB_CONTROLLER in current_node_cache and current_node_cache[constants.FIELD_SUB_CONTROLLER]:
                    overwrite_sub = current_node_cache[constants.FIELD_SUB_CONTROLLER]
                self.logger.debug('DP from intent AbstractFlowTrackingInformation.data_privacy_condition_2.overwrite_dp ({}): {}'.format(dataset_name, overwrite_dp))
                self.logger.debug('DC from intent AbstractFlowTrackingInformation.data_privacy_condition_2.overwrite_dc ({}): {}'.format(dataset_name, overwrite_dc))
                self.logger.debug('SUP DC from intent AbstractFlowTrackingInformation.data_privacy_condition_2.overwrite_sub ({}): {}'.format(dataset_name, overwrite_sub))

                self.logger.info('call concast data privacy ({}) - overwrite_dp: {}'.format(dataset_name, overwrite_dp))
                self.logger.info('call concast data privacy ({}) - overwrite_dc: {}'.format(dataset_name, overwrite_dc))
                self.logger.info('call concast data privacy ({}) - overwrite_sub: {}'.format(dataset_name, overwrite_sub))

                key = '{}_{}_data_privacy'.format(self.app_code, self.user_id)
                data_privacy = self.cache.get_json(key)
                self.logger.debug('AbstractFlowTrackingInformation.data_privacy_condition_2.key: {}'.format(key))
                self.logger.debug('Initial DC DP from dataset AbstractFlowTrackingInformation.data_privacy_condition_2.data_privacy ({}): {}'.format(dataset_name, data_privacy))
                self.logger.debug('Flag all_data_processor from Flowtracking  AbstractFlowTrackingInformation.data_privacy_condition_2.self.all_data_processor ({}): {}'.format(dataset_name, self.all_data_processor))
                self.logger.debug('Flag all_data_controller from Flowtracking  AbstractFlowTrackingInformation.data_privacy_condition_2.self.all_data_controller ({}): {}'.format(dataset_name, self.all_data_controller))

                self.logger.info('call concast data privacy ({}) - data_privacy (cache): {}'.format(dataset_name, data_privacy))
                self.logger.info('call concast data privacy ({}) - all_data_processor (flow_tracking): {}'.format(dataset_name, self.all_data_processor))
                self.logger.info('call concast data privacy ({}) - all_data_controller (flow_tracking): {}'.format(dataset_name, self.all_data_controller))

                if self.all_data_processor: # หลาย processor
                    self.logger.info('call concast data privacy ({}) - case: multi processor'.format(dataset_name))
                    dp_in = ''
                    if overwrite_dp:
                        dp_in = '"{}"'.format(overwrite_dp)
                    elif self.data_processor:
                        dp_in = '"{}"'.format(self.data_processor)
                    if data_privacy and 'data_processor' in data_privacy and data_privacy['data_processor']:
                        for item in data_privacy['data_processor']:
                            if item:
                                if dp_in != '':
                                    dp_in = dp_in + ', '
                                dp_in = dp_in + '"{}"'.format(item)
                    if dp_in:
                        if condition is None or not condition:
                            condition = '{} in ({})'.format(constants.FIELD_DATA_PROCESSOR, dp_in)
                        else:
                            condition += ' and ({} in ({}))'.format(constants.FIELD_DATA_PROCESSOR, dp_in)
                    if self.data_controller:
                        condition += ' and {} = "{}"'.format(constants.FIELD_DATA_CONTROLLER, self.data_controller)
                elif self.all_data_controller: # หลาย controller
                    self.logger.info('call concast data privacy ({}) - case: multi controller'.format(dataset_name))
                    dc_in = ''
                    if overwrite_dc:
                        dc_in = '"{}"'.format(overwrite_dc)
                    # elif self.data_controller:
                    #     dc_in = '"{}"'.format(self.data_controller)
                    if data_privacy and 'data_controller' in data_privacy and data_privacy['data_controller']:
                        for item in data_privacy['data_controller']:
                            if item:
                                if dc_in != '':
                                    dc_in = dc_in + ', '
                                dc_in = dc_in + '"{}"'.format(item)
                    if dc_in:
                        if condition is None or not condition:
                            condition = '{} in ({})'.format(constants.FIELD_DATA_CONTROLLER, dc_in)
                        else:
                            condition += ' and ({} in ({}))'.format(constants.FIELD_DATA_CONTROLLER, dc_in)
                else:
                    self.logger.info('call concast data privacy ({}) - case: other'.format(dataset_name))
                    if overwrite_dp or self.data_processor:
                        if condition is None or not condition:
                            condition = '{} = "{}"'.format(constants.FIELD_DATA_PROCESSOR, overwrite_dp if overwrite_dp else self.data_processor)
                        else:
                            condition += ' and ({} = "{}")'.format(constants.FIELD_DATA_PROCESSOR, overwrite_dp if overwrite_dp else self.data_processor)
                    if overwrite_dc or self.data_controller:
                        if condition is None or not condition:
                            condition = '{} = "{}"'.format(constants.FIELD_DATA_CONTROLLER, overwrite_dc if overwrite_dc else self.data_controller)
                        else:
                            condition += ' and ({} = "{}")'.format(constants.FIELD_DATA_CONTROLLER, overwrite_dc if overwrite_dc else self.data_controller)
                if overwrite_sub or self.sub_controller:
                    if condition is None or not condition:
                        condition = '{} = "{}"'.format(constants.FIELD_SUB_CONTROLLER, overwrite_sub if overwrite_sub else self.sub_controller)
                    else:
                        condition += ' and ({} = "{}")'.format(constants.FIELD_SUB_CONTROLLER, overwrite_sub if overwrite_sub else self.sub_controller)
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            exception_message = exc_type, fname, exc_tb.tb_lineno
            self.logger.error('AbstractFlowTrackingInformation.data_privacy_condition_2.e ({}): {}'.format(dataset_name, e))
            self.logger.error('AbstractFlowTrackingInformation.data_privacy_condition_2.exception_message ({}): {}'.format(dataset_name, exception_message))
            raise e
    
        self.logger.info('call concast data privacy ({}) - condition after concast: {}'.format(dataset_name, condition))
        
        return condition

    def get_wrapper_data_privacy_condition(self, body):
        self.logger.debug('get_wrapper_data_privacy_condition.body : {}'.format(body))
        self.logger.info('get_wrapper_data_privacy_condition.body : {}'.format(body))
        try:
            temp_body = json.dumps(body)
            dataset_name = body.get('dataset_name')
            dc_list = body.get('dc_list', [])
            dp_list = body.get('dp_list', [])
            dc = re.findall('{{(dc)}}',temp_body)
            dp = re.findall('{{(dp)}}',temp_body)
            self.logger.debug('get_wrapper_data_privacy_condition.dc : {}'.format(dc))
            self.logger.info('get_wrapper_data_privacy_condition.dc : {}'.format(dc))
            
            self.logger.debug('get_wrapper_data_privacy_condition.dp : {}'.format(dp))
            self.logger.info('get_wrapper_data_privacy_condition.dp : {}'.format(dp))
            
            is_found_dp_field = False
            is_found_dc_field = False
            
            if dc or dp:
                prepare_condition_dp = ''
                prepare_condition_dc = ''
                # meta_fields = self.getMetaData_2(Launcher(self.logger), dataset_name)
                # self.logger.debug('get_wrapper_data_privacy_condition.meta_fields ({}): {}'.format(dataset_name, meta_fields))
                # self.logger.info('get_wrapper_data_privacy_condition.meta_fields ({}): {}'.format(dataset_name, meta_fields))
                # for meta_field in meta_fields:
                #     if meta_field["name"] == constants.FIELD_DATA_PROCESSOR:
                #         is_found_dp_field = True
                #     elif meta_field["name"] == constants.FIELD_DATA_CONTROLLER:
                #         is_found_dc_field = True
                # is_data_privacy = True
                # self.logger.debug('get_wrapper_data_privacy_condition.call concast data privacy ({}) - is_data_privacy: {}'.format(dataset_name, is_data_privacy))
                # self.logger.info('get_wrapper_data_privacy_condition.call concast data privacy ({}) - is_data_privacy: {}'.format(dataset_name, is_data_privacy))
                # self.logger.debug('get_wrapper_data_privacy_condition.call concast data privacy ({}) - is_found_dp_field: {}'.format(dataset_name, is_found_dp_field))
                # self.logger.info('get_wrapper_data_privacy_condition.call concast data privacy ({}) - is_found_dp_field: {}'.format(dataset_name, is_found_dp_field))
                # self.logger.debug('get_wrapper_data_privacy_condition.call concast data privacy ({}) - is_found_dc_field: {}'.format(dataset_name, is_found_dc_field))
                # self.logger.info('get_wrapper_data_privacy_condition.call concast data privacy ({}) - is_found_dc_field: {}'.format(dataset_name, is_found_dc_field))
                is_data_privacy = True
                if is_data_privacy:
                    overwrite_dp = None
                    overwrite_dc = None
                    current_node_cache = self.getcache_views.post()
                    self.logger.debug('get_wrapper_data_privacy_condition.current_node_cache ({}): {}'.format(dataset_name, current_node_cache))
                    self.logger.info('get_wrapper_data_privacy_condition.current_node_cache ({}): {}'.format(dataset_name, current_node_cache))
                    if constants.FIELD_DATA_PROCESSOR in current_node_cache and current_node_cache[constants.FIELD_DATA_PROCESSOR]:
                        overwrite_dp = current_node_cache[constants.FIELD_DATA_PROCESSOR]
                    if constants.FIELD_DATA_CONTROLLER in current_node_cache and current_node_cache[constants.FIELD_DATA_CONTROLLER]:
                        overwrite_dc = current_node_cache[constants.FIELD_DATA_CONTROLLER]

                    key = '{}_{}_data_privacy'.format(self.app_code, self.user_id)
                    data_privacy = self.cache.get_json(key)
                    self.logger.debug('get_wrapper_data_privacy_condition.key: {}'.format(key))
                    self.logger.info('get_wrapper_data_privacy_condition.key: {}'.format(key))
                    self.logger.debug('Initial DC DP from dataset get_wrapper_data_privacy_condition.data_privacy ({}): {}'.format(dataset_name, data_privacy))
                    self.logger.info('Initial DC DP from dataset get_wrapper_data_privacy_condition.data_privacy ({}): {}'.format(dataset_name, data_privacy))
                    self.logger.debug('Flag all_data_processor from Flowtracking  get_wrapper_data_privacy_condition.self.all_data_processor ({}): {}'.format(dataset_name, self.all_data_processor))
                    self.logger.info('Flag all_data_processor from Flowtracking  get_wrapper_data_privacy_condition.self.all_data_processor ({}): {}'.format(dataset_name, self.all_data_processor))
                    self.logger.debug('Flag all_data_controller from Flowtracking  get_wrapper_data_privacy_condition.self.all_data_controller ({}): {}'.format(dataset_name, self.all_data_controller))
                    self.logger.info('Flag all_data_controller from Flowtracking  get_wrapper_data_privacy_condition.self.all_data_controller ({}): {}'.format(dataset_name, self.all_data_controller))

                    self.logger.debug('call concast data privacy ({}) - data_privacy (cache): {}'.format(dataset_name, data_privacy))
                    self.logger.info('call concast data privacy ({}) - data_privacy (cache): {}'.format(dataset_name, data_privacy))
                    self.logger.debug('call concast data privacy ({}) - all_data_processor (flow_tracking): {}'.format(dataset_name, self.all_data_processor))
                    self.logger.info('call concast data privacy ({}) - all_data_processor (flow_tracking): {}'.format(dataset_name, self.all_data_processor))
                    self.logger.debug('call concast data privacy ({}) - all_data_controller (flow_tracking): {}'.format(dataset_name, self.all_data_controller))
                    self.logger.info('call concast data privacy ({}) - all_data_controller (flow_tracking): {}'.format(dataset_name, self.all_data_controller))

                    if self.all_data_processor and dp:
                        self.logger.debug('call concast data privacy ({}) - case: multi processor'.format(dataset_name))
                        self.logger.info('call concast data privacy ({}) - case: multi processor'.format(dataset_name))
                        dp_in = ''
                        if overwrite_dp:
                            dp_in = '\\"{}\\"'.format(overwrite_dp)
                        elif self.data_processor:
                            dp_in = '\\"{}\\"'.format(self.data_processor)
                        if data_privacy and 'data_processor' in data_privacy and data_privacy['data_processor']:
                            for item in data_privacy['data_processor']:
                                if item:
                                    if dp_in != '':
                                        dp_in = dp_in + ', '
                                    dp_in = dp_in + '\\"{}\\"'.format(item)
                        if dp_in:
                            prepare_condition_dp = '{}'.format(dp_in)
                            if dp_list:
                                prepare_condition_dp = prepare_condition_dp + ', ' + ', '.join(list(map(lambda x: '\\"' + x + '\\"', dp_list)))
                        if dc:
                            if dc_list:
                                if self.data_controller:
                                    prepare_condition_dc += '\\"{}\\"'.format(self.data_controller)
                                prepare_condition_dc = prepare_condition_dc + ', ' + ', '.join(list(map(lambda x: '\\"' + x + '\\"', dc_list))) if prepare_condition_dc else ', ' + ', '.join(list(map(lambda x: '\\"' + x + '\\"', dc_list)))
                            elif self.data_controller:
                                prepare_condition_dc += '\\"{}\\"'.format(self.data_controller)
                    elif self.all_data_controller and dc:
                        self.logger.debug('call concast data privacy ({}) - case: multi controller'.format(dataset_name))
                        self.logger.info('call concast data privacy ({}) - case: multi controller'.format(dataset_name))
                        dc_in = ''
                        if overwrite_dc:
                            dc_in = '\\"{}\\"'.format(overwrite_dc)
                        if data_privacy and 'data_controller' in data_privacy and data_privacy['data_controller']:
                            for item in data_privacy['data_controller']:
                                if item:
                                    if dc_in != '':
                                        dc_in = dc_in + ', '
                                    dc_in = dc_in + '\\"{}\\"'.format(item)
                        if dc_in:
                            prepare_condition_dc = '{}'.format(dc_in)
                            if dc_list:
                                prepare_condition_dc = prepare_condition_dc + ', ' + ', '.join(list(map(lambda x: '\\"' + x + '\\"', dc_list)))
                        if dp:
                            if dp_list:
                                if self.data_processor:
                                    prepare_condition_dp += '\\"{}\\"'.format(self.data_processor)
                                prepare_condition_dp = prepare_condition_dp + ', ' + ', '.join(list(map(lambda x: '\\"' + x + '\\"', dp_list))) if prepare_condition_dp else ', ' + ', '.join(list(map(lambda x: '\\"' + x + '\\"', dp_list)))
                            elif self.data_processor:
                                prepare_condition_dp += '\\"{}\\"'.format(self.data_processor)
                    else:
                        self.logger.debug('call concast data privacy ({}) - case: other'.format(dataset_name))
                        self.logger.info('call concast data privacy ({}) - case: other'.format(dataset_name))
                        if dp:
                            if dp_list:
                                if self.data_processor:
                                    prepare_condition_dp += '\\"{}\\"'.format(self.data_processor)
                                prepare_condition_dp = prepare_condition_dp + ', ' + ', '.join(list(map(lambda x: '\\"' + x + '\\"', dp_list))) if prepare_condition_dp else ', ' + ', '.join(list(map(lambda x: '\\"' + x + '\\"', dp_list)))
                            elif self.data_processor:
                                prepare_condition_dp += '\\"{}\\"'.format(self.data_processor)
                        if dc:
                            if dc_list:
                                if self.data_controller:
                                    prepare_condition_dc += '\\"{}\\"'.format(self.data_controller)
                                prepare_condition_dc = prepare_condition_dc + ', ' + ', '.join(list(map(lambda x: '\\"' + x + '\\"', dc_list))) if prepare_condition_dc else ', ' + ', '.join(list(map(lambda x: '\\"' + x + '\\"', dc_list)))
                            elif self.data_controller:
                                prepare_condition_dc += '\\"{}\\"'.format(self.data_controller)
                if dc:
                    replaceDcStr = '{{' + 'dc' + '}}'
                    temp_body = temp_body.replace(replaceDcStr, str(prepare_condition_dc))
                if dp:
                    replaceDpStr = '{{' + 'dp' + '}}'
                    temp_body = temp_body.replace(replaceDpStr, str(prepare_condition_dp))
            return json.loads(temp_body)
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            exception_message = exc_type, fname, exc_tb.tb_lineno
            self.logger.error('get_wrapper_data_privacy_condition.e ({}): {}'.format(dataset_name, e))
            self.logger.error('get_wrapper_data_privacy_condition.exception_message ({}): {}'.format(dataset_name, exception_message))
            raise e
            
