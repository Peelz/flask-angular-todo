import json
import os
import sys
import redis
import requests
import re
import uuid
import time
import pytz

from ast import literal_eval

from flask import current_app as app
from flask import jsonify, request, session

from datetime import datetime
# from dateutil.parser import parse

from app.commons.services.cache import Cache
from app.commons.services.log import Logger

from app.commons import constants
from app.commons.utils.tool import Tool
from app.commons.login_data import LoginData

import time

from app.commons.jaeger_util import JaegerUtil

class PreSubmit():

    
    def __init__(self, workflowkey, flow_tracking=None, reference_id=None):
        self.cache_api = Cache()

        self.login_data = LoginData()

        self.workflowkey = self.login_data.get_work_flow_key()

        if flow_tracking is None:
            self.flow_tracking = self.login_data.get_flow_tracking()
        else:
            self.flow_tracking = flow_tracking

        self.reference_id = reference_id

        login_info = self.flow_tracking.get('login_info') or self.flow_tracking.get('appInfo')
        self.userid = login_info.get('user_id')
        self.appid = int(login_info.get('app_id'))

        self.flow_code = self.flow_tracking.get('code')
        self.log = Logger()

        self.log.debug('PreSubmit.__init__')
        self.log.info('init call pre submit')


    def convertTypeData(self, expectType, value):
        try:
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
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            exception_message = exc_type, fname, exc_tb.tb_lineno
            self.log.error('PreSubmit.convertTypeData.e: {}'.format(e))
            self.log.error('PreSubmit.convertTypeData.exception_message: {}'.format(exception_message))
            raise Exception(e)
        return value


    def convertTypeDataForJson(self, expectType, value):
        try:
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
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            exception_message = exc_type, fname, exc_tb.tb_lineno
            self.log.error('PreSubmit.convertTypeDataForJson.e: {}'.format(e))
            self.log.error('PreSubmit.convertTypeDataForJson.exception_message: {}'.format(exception_message))
            raise Exception(e)
        return value

    def convertObjectToAssociateArray(self, __object):
        parameter = ''
        try:
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
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            exception_message = exc_type, fname, exc_tb.tb_lineno
            self.log.error('PreSubmit.convertObjectToAssociateArray.e: {}'.format(e))
            self.log.error('PreSubmit.convertObjectToAssociateArray.exception_message: {}'.format(exception_message))
            raise Exception(e)
        return parameter


    def filterInParamsFormat(self, element):
        name = element.get('name')
        if name[0:2] == '{% templatetag openvariable %}' and name[-2] + name[-1] == '{% templatetag closevariable %}':
            return True
    

    def filterOutParamsFormat(self, element):
        name = element.get('name')
        if name[0:2] == '{% templatetag openvariable %}' and name[-2] + name[-1] == '{% templatetag closevariable %}':
            return False
        else:
            return True


    def putPresubmit(self, sequence, request, success_with_message=False):
        data = self.getPresubmit()
        key = 'PRESUBMIT_' + self.workflowkey
        
        data['procedure_step'] = {
            'sequence': sequence,
            'request': request,
            'success_with_message': success_with_message,
        }

        self.cache_api.put_json(key, data)


    def getPresubmit(self):
        key = 'PRESUBMIT_' + self.workflowkey
        try:
            response_cache = self.cache_api.get_json(key)
        except Exception:
            response_cache = None
        
        return response_cache

    
    def deletePresubmit(self):
        key = 'PRESUBMIT_' + self.workflowkey
        self.cache_api.delete(key)


    def getFlowTracking(self):
        key = 'FLOWTRACKING_' + self.workflowkey
        
        try:
            response_cache = self.cache_api.get_json(key)
        except Exception:
            response_cache = {}
        
        return response_cache


    def getHeader(self):
        header = {}
        try:
            login_info = self.flow_tracking.get('login_info')
            client_key = app.config.get('MULE_CLIENT_ID')
            client_secret_key = app.config.get('MULE_CLIENT_SECRET')
            header = {
                'header': [{
                    'key': 'app-meta',
                    'value': json.dumps({
                        'user_id': login_info.get('user_id'),
                        'user_name': login_info.get('username'),
                        'state': app.config.get('CURRENT_STATE'),
                        'request_datetime': datetime.now().strftime("%d-%m-%Y %H:%M:%S"),
                        'log_session_id': session.get('reference_id'),
                        'sub_state': app.config.get('CURRENT_SUB_STATE'),
                        'app_no': str(self.appid),
                        'flow_session_id': self.workflowkey,
                    })
                },{
                    'key': 'client_id',
                    'value': client_key 
                },{
                    'key': 'client_secret',
                    'value': client_secret_key
                }],
                'client_id': client_key,
                'client_secret': client_secret_key,
            }
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            exception_message = exc_type, fname, exc_tb.tb_lineno
            self.log.error('PreSubmit.getHeader.e: {}'.format(e))
            self.log.error('PreSubmit.getHeader.exception_message: {}'.format(exception_message))
            raise Exception(e)
        return header


    def createUrl(self, params, url, requestBody, flow_tracking):
        try:
            parameters = list()
            request_data = requestBody.get('request_data', [])

            parameter_all = re.findall('{% templatetag openvariable %}(.+?){% templatetag closevariable %}', url)

            # unique
            for item in parameter_all:
                if item not in parameters:
                    parameters.append(item)
            
            for parameter in parameters:
                result_value = ''
                if params:
                    for param_config in params:
                        if '{% templatetag openvariable %}' + parameter + '{% templatetag closevariable %}' == param_config.get('name'):
                            #create value
                            value_type = param_config.get('vtype')
                            value_param = param_config.get('value')
                            data_type = param_config.get('type')

                            if value_type == 'datafield':
                                if value_param is None or value_param == '':
                                    result_value = ''
                                else:
                                    value_param_element = value_param.split('.')

                                    request = request_data
                                    for element in value_param_element:
                                        if isinstance(request, list):
                                            result_value = request[0].get(element)
                                        else:
                                            result_value = request.get(element)

                                        if result_value is None or result_value == '':
                                            break
                                        
                                        request = result_value
                    
                            elif value_type == 'constant':
                                result_value = value_param
                            elif value_type == 'system':
                                result_value = ''

                                if value_param == 'APP_CODE':
                                    result_value = flow_tracking.get('login_info', {}).get('application_number') or flow_tracking.get('login_info', {}).get('app_id')
                                elif value_param == 'BRANCH_INFO_ID':
                                    result_value = flow_tracking.get('login_info', {}).get('user_branch_id') or flow_tracking.get('login_info', {}).get('branch')
                                elif value_param == 'BRANCH_CODE':
                                    result_value = flow_tracking.get('login_info', {}).get('user_branch_code')
                                elif value_param == 'USER_ID':
                                    result_value = flow_tracking.get('login_info', {}).get('user_id') or ''
                                elif value_param == 'USER_UCID':
                                    result_value = flow_tracking.get('login_info', {}).get('user_ucid') or ''
                                elif value_param == 'EMPLOYEE_ID':
                                    result_value = flow_tracking.get('login_info', {}).get('employee_id')
                                elif value_param == 'USERNAME':
                                    result_value = flow_tracking.get('login_info', {}).get('username') or ''
                                elif value_param == 'ERM_ROLE':
                                    result_value = flow_tracking.get('login_info', {}).get('enterprise_role_code') or flow_tracking.get('login_info', {}).get('erm_role')
                                elif value_param == 'ERM_ROLE_ID':
                                    result_value = flow_tracking.get('login_info', {}).get('enterprise_role_id')
                                elif value_param == 'COMPANY_INFOMATION':
                                    result_value = flow_tracking.get('login_info', {}).get('company_ucid')
                                elif value_param == 'COMPANY_CODE':
                                    result_value = flow_tracking.get('login_info', {}).get('company_code')
                                elif value_param == 'REGISTRATION_SERVICE_ID':
                                    result_value = flow_tracking.get('login_info', {}).get('registration_service_id')
                                elif value_param == 'ORGANIZATION_UNIT_ID':
                                    result_value = flow_tracking.get('login_info', {}).get('user_organization_unit_id')
                                elif value_param == 'ORGANIZATION_UNIT_CODE':
                                    result_value = flow_tracking.get('login_info', {}).get('user_organization_unit_code')
                                elif value_param == 'DATA_CONTROLLER':
                                    result_value = flow_tracking.get('login_info', {}).get('data_controller')
                                elif value_param == 'SUB_CONTROLLER':
                                    result_value = flow_tracking.get('login_info', {}).get('sub_controller')
                                elif value_param == 'DATA_PROCESSOR':
                                    result_value = flow_tracking.get('login_info', {}).get('data_processor')
                                elif value_param == 'FLOW_SESSION_ID':
                                    result_value = flow_tracking.get('workflow_key')
                                elif value_param == 'LOG_SESSION_ID':
                                    result_value = flow_tracking.get('login_info', {}).get('log_session_id')
                                    if result_value is None:
                                        result_value = str(uuid.uuid4())
                                    result_value = self.workflowkey
                                elif value_param == 'CURRENT_DATE':
                                    if data_type in ['number', 'integer', 'decimal', 'datetime']:
                                        current = datetime.now(pytz.timezone("Asia/Bangkok")) \
                                                    .replace(hour=0, minute=0, second=0, microsecond=0) \
                                                    .astimezone(pytz.utc)
                                        result_value = round(current.timestamp()) * 1000
                                        # result_value = int(round(time.time() * 1000))
                                    elif data_type == 'text':
                                        result_value = datetime.now().strftime("%d/%m/%Y")
                                elif value_param == 'CURRENT_TIME':
                                    if data_type in ['number', 'integer', 'decimal', 'datetime']:
                                        result_value = int(round(time.time() * 1000))
                                    elif data_type == 'text':
                                        result_value = datetime.now().strftime("%H:%M:%S")
                                elif value_param == 'CURRENT_DATETIME':
                                    if data_type in ['number', 'integer', 'decimal', 'datetime']:
                                        result_value = int(round(time.time() * 1000))
                                    elif data_type == 'text':
                                        result_value = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
                            else:
                                result_value = ''
                            break

                result_value = str(result_value)
                url = url.replace('{% templatetag openvariable %}' + parameter + '{% templatetag closevariable %}', result_value)
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            exception_message = exc_type, fname, exc_tb.tb_lineno
            self.log.error('PreSubmit.createUrl.e: {}'.format(e))
            self.log.error('PreSubmit.createUrl.exception_message: {}'.format(exception_message))
            raise Exception(e)
        return url


    def createBody(self, params, requestBody, flow_tracking):
        result = dict()
        try:
            # name_list = [d['name'] if 'name' in d else None for d in params]

            # create structer
            for item in params:
                name = item.get('name')
                if name is None:
                    continue
                
                parameter = ''
                request_data = requestBody.get('request_data', {})
                if isinstance(request_data, list):
                    request_data = request_data[0]

                hierarchy = name.split('.')

                for index, node in enumerate(hierarchy):
                    node = node.replace('[]', '[0]')
                    index_list = re.findall('\[(.+?)\]', node)

                    parameter += self.convertObjectToAssociateArray(node)
                    try:
                        eval('result' + parameter)
                        continue
                    except Exception:
                        pass

                    if index_list:
                        field_value = list()
                        index_length = len(index_list[0])
                        index_value = int(index_list[0])
                        
                        for x in range(index_value + 1):
                            try:
                                field = eval('result' + parameter[:-(index_length + 2)] + '[x]')
                            except Exception:
                                field = dict()

                            field_value.append(field)
                        exec('result' + parameter[:-(index_length + 2)] + ' = {}'.format(field_value))
                    else:
                        if index < len(hierarchy) - 1:
                            field_value = dict()
                        else:
                            # get Value
                            value_type = item.get('vtype')
                            value = item.get('value')
                            multiple = item.get('multiple') or False
                            parameter_type = item.get('type')

                            field_value = None
                            object_multiple_constant = False

                            if value_type == 'datafield': 
                                if value is not None and value:
                                    field_name = self.convertObjectToAssociateArray(value)
                                    try:
                                        field_value = eval('request_data' + field_name)
                                    except Exception:
                                        pass
                            elif value_type == 'constant':
                                if multiple == True:
                                    if parameter_type == 'object':
                                        value = '[' + value + ']'
                                        object_multiple_constant = True
                                    else:
                                        value = value.split(',')
                                        value = [item.strip() for item in value]

                                field_value = value
                            elif value_type == 'system':
                                if value == 'APP_CODE':
                                    field_value = flow_tracking.get('login_info', {}).get('application_number') or flow_tracking.get('login_info', {}).get('app_id')
                                elif value == 'BRANCH_INFO_ID':
                                    field_value = flow_tracking.get('login_info', {}).get('user_branch_id') or flow_tracking.get('login_info', {}).get('branch')
                                elif value == 'BRANCH_CODE':
                                    field_value = flow_tracking.get('login_info', {}).get('user_branch_code')
                                elif value == 'USER_ID':
                                    field_value = flow_tracking.get('login_info', {}).get('user_id')
                                elif value == 'USER_UCID':
                                    field_value = flow_tracking.get('login_info', {}).get('user_ucid') or ''
                                elif value == 'EMPLOYEE_ID':
                                    field_value = flow_tracking.get('login_info', {}).get('employee_id')
                                elif value == 'USERNAME':
                                    field_value = flow_tracking.get('login_info', {}).get('username')
                                elif value == 'ERM_ROLE':
                                    field_value = flow_tracking.get('login_info', {}).get('enterprise_role_code') or flow_tracking.get('login_info', {}).get('erm_role')
                                elif value == 'ERM_ROLE_ID':
                                    field_value = flow_tracking.get('login_info', {}).get('enterprise_role_id')
                                elif value == 'COMPANY_INFOMATION':
                                    field_value = flow_tracking.get('login_info', {}).get('company_ucid')
                                elif value == 'COMPANY_CODE':
                                    field_value = flow_tracking.get('login_info', {}).get('company_code')
                                elif value == 'REGISTRATION_SERVICE_ID':
                                    field_value = flow_tracking.get('login_info', {}).get('registration_service_id')
                                elif value == 'ORGANIZATION_UNIT_ID':
                                    field_value = flow_tracking.get('login_info', {}).get('user_organization_unit_id')
                                elif value == 'ORGANIZATION_UNIT_CODE':
                                    field_value = flow_tracking.get('login_info', {}).get('user_organization_unit_code')
                                elif value == 'DATA_CONTROLLER':
                                    field_value = flow_tracking.get('login_info', {}).get('data_controller')
                                elif value == 'SUB_CONTROLLER':
                                    field_value = flow_tracking.get('login_info', {}).get('sub_controller')
                                elif value == 'DATA_PROCESSOR':
                                    field_value = flow_tracking.get('login_info', {}).get('data_processor')
                                elif value == 'FLOW_SESSION_ID':
                                    field_value = flow_tracking.get('workflow_key')
                                elif value == 'LOG_SESSION_ID':
                                    field_value = flow_tracking.get('login_info', {}).get('log_session_id')
                                    if field_value is None:
                                        field_value = str(uuid.uuid4())
                                    field_value = self.workflowkey
                                elif value == 'CURRENT_DATE':
                                    if parameter_type in ['number', 'integer', 'decimal', 'datetime']:
                                        current = datetime.now(pytz.timezone("Asia/Bangkok")) \
                                                    .replace(hour=0, minute=0, second=0, microsecond=0) \
                                                    .astimezone(pytz.utc)
                                        field_value = round(current.timestamp()) * 1000
                                        # field_value = int(round(time.time() * 1000))
                                    elif parameter_type == 'text':
                                        field_value = datetime.now().strftime("%d/%m/%Y")
                                elif value == 'CURRENT_TIME':
                                    if parameter_type in ['number', 'integer', 'decimal', 'datetime']:
                                        field_value = int(round(time.time() * 1000))
                                    elif parameter_type == 'text':
                                        field_value = datetime.now().strftime("%H:%M:%S")
                                elif value == 'CURRENT_DATETIME':
                                    if parameter_type in ['number', 'integer', 'decimal', 'datetime']:
                                        field_value = int(round(time.time() * 1000))
                                    elif parameter_type == 'text':
                                        field_value = datetime.now().strftime("%d/%m/%Y %H:%M:%S")

                            if field_value is not None:
                                try:
                                    if multiple == True:
                                        result_list = list()
                                        if isinstance(field_value, list):
                                            for item in field_value:
                                                item_value = self.convertTypeData(parameter_type, item)
                                                result_list.append(item_value)
                                        elif object_multiple_constant == True:
                                            value = self.convertTypeData(parameter_type, field_value)
                                            result_list = value
                                        else:
                                            value = self.convertTypeData(parameter_type, field_value)
                                            result_list.append(value)

                                        field_value = result_list
                                    else:
                                        field_value = self.convertTypeDataForJson(parameter_type, field_value)
                                except Exception as exc:
                                    raise Exception(exc)

                        exec('result' + parameter + ' = {}'.format(field_value))
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            exception_message = exc_type, fname, exc_tb.tb_lineno
            self.log.error('PreSubmit.createBody.e: {}'.format(e))
            self.log.error('PreSubmit.createBody.exception_message: {}'.format(exception_message))
            raise Exception(e)
        return result

            
    def getValue(self, propertyName, body):
        value = None
        try:
            if propertyName is None:
                return None

            property_name = self.convertObjectToAssociateArray(propertyName)

            try:
                value = eval('body' + property_name)
            except Exception:
                value = None
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            exception_message = exc_type, fname, exc_tb.tb_lineno
            self.log.error('PreSubmit.getValue.e: {}'.format(e))
            self.log.error('PreSubmit.getValue.exception_message: {}'.format(exception_message))
            raise Exception(e)
        
        return value


    def mappingRequest(self, mappingValue, request, response):
        result = dict()
        try:
            if mappingValue:
                for mapping in mappingValue:
                    vname = mapping.get('vname')
                    name = mapping.get('name')
                    name_type = mapping.get('type')
                    multiple = mapping.get('multiple') or False

                    self.log.debug('PreSubmit.mappingRequest.before.getValue.vname: {}'.format(vname))
                    self.log.debug('PreSubmit.mappingRequest.before.getValue.response: {}'.format(response))
                    value = self.getValue(vname, response)
                    self.log.debug('PreSubmit.mappingRequest.after.getValue.value: {}'.format(value))
                    if value is not None and value:
                        if multiple == True:
                            result_list = list()
                            if not (isinstance(value, list)):
                                result_list.append(value)
                            else:
                                result_list = value

                            value = list()
                            for item in result_list:
                                self.log.debug('PreSubmit.mappingRequest.before.convertTypeData.name_type: {}'.format(name_type))
                                self.log.debug('PreSubmit.mappingRequest.before.convertTypeData.item: {}'.format(item))
                                item_value = self.convertTypeData(name_type, item)
                                self.log.debug('PreSubmit.mappingRequest.after.convertTypeData.item_value: {}'.format(item_value))
                                value.append(item_value)
                        else:
                            if isinstance(value, list):
                                raise Exception("Mapping Request: {} is not multiple value but response value is multiple value".format(name))
                            
                            self.log.debug('PreSubmit.mappingRequest.before.convertTypeDataForJson.name_type: {}'.format(name_type))
                            self.log.debug('PreSubmit.mappingRequest.before.convertTypeDataForJson.value: {}'.format(value))
                            value = self.convertTypeDataForJson(name_type, value)
                            self.log.debug('PreSubmit.mappingRequest.after.convertTypeDataForJson.value: {}'.format(value))

                    self.log.debug('PreSubmit.mappingRequest.before.convertObjectToAssociateArray.name: {}'.format(name))
                    name_field = self.convertObjectToAssociateArray(name)
                    self.log.debug('PreSubmit.mappingRequest.after.convertObjectToAssociateArray.name_field: {}'.format(name_field))
                    name_field = '[\'request_data\'][0]' + name_field
                    self.log.debug('PreSubmit.mappingRequest.after.convertObjectToAssociateArray.name_field2: {}'.format(name_field))

                    exec_data = 'request' + name_field + ' = {}'.format(value)
                    self.log.debug('PreSubmit.mappingRequest.exec_data: {}'.format(exec_data))

                    exec('request' + name_field + ' = {}'.format(value))
                
                result = request
            else:
                result = request
            
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            exception_message = exc_type, fname, exc_tb.tb_lineno
            self.log.error('PreSubmit.mappingRequest.e: {}'.format(e))
            self.log.error('PreSubmit.mappingRequest.exception_message: {}'.format(exception_message))
            raise Exception(e)

        self.log.debug('PreSubmit.mappingRequest.result: {}'.format(result))
        return result
        


    def preSubmitVerify(self, request=None, ignore=False, is_multiple=False):
        try:
            self.log.debug('PreSubmit.preSubmitVerify.request.in: {}'.format(request))
            self.log.debug('PreSubmit.preSubmitVerify.ignore: {}'.format(ignore))
            self.log.debug('PreSubmit.preSubmitVerify.is_multiple: {}'.format(is_multiple))

            self.log.info('call pre submit - request: {}'.format(request))
            self.log.info('call pre submit - ignore: {}'.format(ignore))
            self.log.info('call pre submit - is_multiple: {}'.format(is_multiple))

            presubmit_cache = None
            flow_tracking = None
            procedure_sequence = 0
            procedure_success_with_message = False

            presubmit_cache = self.getPresubmit()

            self.log.debug('PreSubmit.preSubmitVerify.presubmit_cache: {}'.format(presubmit_cache))

            self.log.info('call pre submit - presubmit_cache: {}'.format(presubmit_cache))

            if presubmit_cache is not None and presubmit_cache:
                procedure_step = presubmit_cache.get('procedure_step')
                procedure_sequence = procedure_step.get('sequence', 0)
                procedure_request = procedure_step.get('request') or dict()
                procedure_success_with_message = procedure_step.get('success_with_message') or False

                if procedure_request:
                    request = procedure_request

            # get flow tracking
            flow_tracking = self.flow_tracking

            if flow_tracking is None or not flow_tracking:
                return "success", "[Pre-submit] Can not get flow tracking cache", None, None

            # get presubmit services
            # current_instance = flow_tracking.get('current_instance')
            current_reference_uuid = self.flow_tracking.get('current_reference_uuid')
            instances = flow_tracking.get('instances') or list()

            self.log.debug('PreSubmit.preSubmitVerify.current_reference_uuid: {}'.format(current_reference_uuid))
            self.log.debug('PreSubmit.preSubmitVerify.instances: {}'.format(instances))
            
            if not instances:
                return "success", "[Pre-submit] Not found instances in flow tracking", None, None
            
            instance = self.getInstance(current_reference_uuid, instances)

            self.log.debug('PreSubmit.preSubmitVerify.instance: {}'.format(instance))
            
            # instance = dict()
            # for item in instances:
            #     if current_instance == item.get('sequence'):
            #         sub_sequence = item.get('sub_sequence') or 0
            #         if sub_sequence > 0:
            #             selected_flag = item.get('selected') or False
            #             if selected_flag == True:
            #                 instance = item
            #                 break
            #         else:
            #             instance = item
            #             break
            
            pre_submit_services = instance.get('pre_submit_services') or list()

            self.log.debug('PreSubmit.preSubmitVerify.pre_submit_services: {}'.format(pre_submit_services))

            self.log.info('call pre submit - pre_submit_services: {}'.format(pre_submit_services))

            # not found pre-submit
            if not pre_submit_services:
                return "success", "[Pre-submit] Not found pre-submit services", None, None

            # pre-submit service loop
            for index, pre_submit in enumerate(pre_submit_services):
                self.log.debug('PreSubmit.preSubmitVerify.index.pre_submit: {}, {}'.format(index, pre_submit))
                self.log.debug('PreSubmit.preSubmitVerify.index.procedure_sequence: {}, {}'.format(index, procedure_sequence))
                if procedure_sequence > 0:
                    if index == 0:
                        self.deletePresubmit()

                    if ignore == True or procedure_success_with_message == True:
                        if index < procedure_sequence:
                            continue
                    else:
                        return "failure and clear cache success", "", None, None
                        
                # get parameter
                pre_submit_name = pre_submit.get('name')
                url = pre_submit.get('url')
                sequence = pre_submit.get('sequence')
                request_params = pre_submit.get('request', {}).get('params') or list()
                method = pre_submit.get('request', {}).get('http_method')
                response_success_condition = pre_submit.get('response', {}).get('success') or dict()
                failure_response_message = pre_submit.get('response', {}).get('message') or dict()
                validity = pre_submit.get('validity') or dict()
                bypass_validation = pre_submit.get('validity', {}).get('bypass_validation') or False
                valid_condition = pre_submit.get('validity', {}).get('valid_condition') or dict()
                warning_condition = pre_submit.get('validity', {}).get('warning_condition') or dict()
                message_property =  pre_submit.get('validity', {}).get('message') or dict()
                mapping_value = pre_submit.get('mapping_value') or list()

                self.log.debug('PreSubmit.preSubmitVerify.index.pre_submit_name: {}, {}'.format(index, pre_submit_name))
                self.log.debug('PreSubmit.preSubmitVerify.index.url: {}, {}'.format(index, url))
                self.log.debug('PreSubmit.preSubmitVerify.index.sequence: {}, {}'.format(index, sequence))
                self.log.debug('PreSubmit.preSubmitVerify.index.request_params: {}, {}'.format(index, request_params))
                self.log.debug('PreSubmit.preSubmitVerify.index.method: {}, {}'.format(index, method))
                self.log.debug('PreSubmit.preSubmitVerify.index.response_success_condition: {}, {}'.format(index, response_success_condition))
                self.log.debug('PreSubmit.preSubmitVerify.index.failure_response_message: {}, {}'.format(index, failure_response_message))
                self.log.debug('PreSubmit.preSubmitVerify.index.validity: {}, {}'.format(index, validity))
                self.log.debug('PreSubmit.preSubmitVerify.index.bypass_validation: {}, {}'.format(index, bypass_validation))
                self.log.debug('PreSubmit.preSubmitVerify.index.valid_condition: {}, {}'.format(index, valid_condition))
                self.log.debug('PreSubmit.preSubmitVerify.index.warning_condition: {}, {}'.format(index, warning_condition))
                self.log.debug('PreSubmit.preSubmitVerify.index.message_property: {}, {}'.format(index, message_property))
                self.log.debug('PreSubmit.preSubmitVerify.index.mapping_value: {}, {}'.format(index, mapping_value))

                self.log.info('call pre submit ({}, {}) - pre_submit_name: {}'.format(index, pre_submit_name, pre_submit_name))
                self.log.info('call pre submit ({}, {}) - url: {}'.format(index, pre_submit_name, url))
                self.log.info('call pre submit ({}, {}) - sequence: {}'.format(index, pre_submit_name, sequence))
                self.log.info('call pre submit ({}, {}) - request_params: {}'.format(index, pre_submit_name, request_params))
                self.log.info('call pre submit ({}, {}) - method: {}'.format(index, pre_submit_name, method))
                self.log.info('call pre submit ({}, {}) - response_success_condition: {}'.format(index, pre_submit_name, response_success_condition))
                self.log.info('call pre submit ({}, {}) - failure_response_message: {}'.format(index, pre_submit_name, failure_response_message))
                self.log.info('call pre submit ({}, {}) - validity: {}'.format(index, pre_submit_name, validity))
                self.log.info('call pre submit ({}, {}) - bypass_validation: {}'.format(index, pre_submit_name, bypass_validation))
                self.log.info('call pre submit ({}, {}) - valid_condition: {}'.format(index, pre_submit_name, valid_condition))
                self.log.info('call pre submit ({}, {}) - warning_condition: {}'.format(index, pre_submit_name, warning_condition))
                self.log.info('call pre submit ({}, {}) - message_property: {}'.format(index, pre_submit_name, message_property))
                self.log.info('call pre submit ({}, {}) - mapping_value: {}'.format(index, pre_submit_name, mapping_value))

                if method is None or not method:
                    self.log.debug("[Pre-submit] {} is failure: Method not found".format(pre_submit_name))

                    self.log.info('call pre submit ({}, {}) - method not found'.format(index, pre_submit_name))

                    return "failure", "Internal Error: Method not found", None, pre_submit_name
                if url is None or not url:
                    self.log.debug("[Pre-submit] {} is failure: URL not found".format(pre_submit_name))

                    self.log.info('call pre submit ({}, {}) - url not found'.format(index, pre_submit_name))

                    return "failure", "Internal Error: URL not found", None, pre_submit_name
                
                # get header 
                header_element = self.getHeader()
                header = header_element.get('header')
                client_id = header_element.get('client_id')
                client_secret = header_element.get('client_secret')

                # filter param for create url and body
                filter_in_parameter_format = list(filter(self.filterInParamsFormat, request_params))
                filter_out_parameter_format = list(filter(self.filterOutParamsFormat, request_params))

                # create URL
                url = self.createUrl(filter_in_parameter_format, url, request, flow_tracking)

                self.log.debug('PreSubmit.preSubmitVerify.index.createUrl.url: {}, {}'.format(index, url))

                self.log.debug("[Pre-submit][{}] URL: {}".format(pre_submit_name, url))

                # create body
                try:
                    body = self.createBody(filter_out_parameter_format, request, flow_tracking)
                    self.log.debug('PreSubmit.preSubmitVerify.index.body: {}, {}'.format(index, body))
                except Exception as exc:
                    self.log.debug('PreSubmit.preSubmitVerify.index.exc: {}, {}'.format(index, exc))
                    self.log.debug("[Pre-submit] {} is failure: {}".format(pre_submit_name, str(exc)))

                    self.log.info('call pre submit ({}, {}) - Internal Error (Create Body): {}'.format(index, pre_submit_name, exc))

                    return "failure", "Internal Error (Create Body): {}".format(str(exc)), None, pre_submit_name

                # send request
                method = method.upper()

                self.log.debug("[Pre-submit][{}] Body: {}".format(pre_submit_name, body))

                try:
                    cafile = app.config.get('SSL_CERT_FILE', '')

                    headers = dict()
                    headers['Content-type'] = 'application/json'

                    for header_element in header:
                        headers[header_element['key']] = header_element['value']

                    self.log.debug('PreSubmit.preSubmitVerify.call.url: {}'.format(url))
                    self.log.debug('PreSubmit.preSubmitVerify.call.headers: {}'.format(headers))
                    self.log.debug('PreSubmit.preSubmitVerify.call.body: {}'.format(body))
                    self.log.debug('PreSubmit.preSubmitVerify.call.method: {}'.format(method))

                    self.log.info('call pre submit ({}, {}) - request service - url: {}'.format(index, pre_submit_name, url))
                    headers_log = Tool.remove_sensitive_key_for_logging(headers)
                    self.log.info('call pre submit ({}, {}) - request service - headers_log: {}'.format(index, pre_submit_name, headers_log))
                    self.log.info('call pre submit ({}, {}) - request service - body: {}'.format(index, pre_submit_name, body))
                    self.log.info('call pre submit ({}, {}) - request service - method: {}'.format(index, pre_submit_name, method))
                    
                    start_time = time.time()

                    if JaegerUtil.tracer:
                        with JaegerUtil.tracer.start_active_span(url) as scope:
                            if scope and scope.span:
                                scope.span.log_kv({'event': 'pre_submit'})
                                JaegerUtil.active_span = scope.span
                                headers = JaegerUtil.inject_header(None, url, method, headers)
                                if method == 'POST':
                                    response = requests.post(url, headers=headers, data=json.dumps(body), verify=cafile)
                                elif method == 'GET':
                                    response = requests.get(url, headers=headers, verify=cafile)
                                elif method == 'PUT':
                                    response = requests.put(url, headers=headers, data=json.dumps(body), verify=cafile)
                                elif method == 'DELETE':
                                    response = requests.delete(url, headers=headers, data=json.dumps(body), verify=cafile)
                                else:
                                    raise Exception("Method [{}] not support".format(method))

                    end_time = time.time()
                    used_time = (end_time - start_time) * 1000
                    
                    self.log.debug('PreSubmit.preSubmitVerify.call.used_time: {}'.format(used_time))
                    self.log.info('call pre submit ({}, {}) - request service - used_time: {}'.format(index, pre_submit_name, used_time))

                    try:

                        self.log.debug('PreSubmit.preSubmitVerify.call.response.text: {}'.format(response.text))
                        if response.status_code != 200:
                            self.log.error('PreSubmit.preSubmitVerify.call.error.response.text: {}'.format(response.text))

                        self.log.debug('PreSubmit.preSubmitVerify.call.response.json(): {}'.format(response.json()))

                        response_json = response.json()
                        response_json_log = Tool.remove_sensitive_key_for_logging(response_json)

                        self.log.info('call pre submit ({}, {}) - request service - response_json_log: {}'.format(index, pre_submit_name, response_json_log))

                    except Exception as e:
                        exc_type, exc_obj, exc_tb = sys.exc_info()
                        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                        exception_message = exc_type, fname, exc_tb.tb_lineno
                        self.log.error('PreSubmit.preSubmitVerify.call.e: {}'.format(e))
                        self.log.error('PreSubmit.preSubmitVerify.call.exception_message: {}'.format(exception_message))

                    response.raise_for_status()

                except Exception as exc:
                    self.log.debug('PreSubmit.preSubmitVerify.call.exc: {}'.format(exc))
                    self.log.debug("[Pre-submit] {} is failure: {}".format(pre_submit_name, str(exc)))

                    self.log.info('call pre submit ({}, {}) - request service - exc: {}'.format(index, pre_submit_name, exc))

                    response = response.text
                    if response is None or not response:
                        return_message = 'Receive Status Code: {} from {}'.format(response.status_code, url)
                        return "failure", "{} ({})".format(str(exc), return_message), None, pre_submit_name

                    try:
                        resp = json.loads(response)
                    except Exception:
                        return_message = 'Receive Status Code: {} {} from {}'.format(response.status_code, response, url)
                        return "failure", "{} ({})".format(str(exc), return_message), None, pre_submit_name

                    if failure_response_message:
                        return_message = self.getValue(failure_response_message, resp) or 'API Failure'
                        return "failure", "{} ({})".format(str(exc), return_message), None, pre_submit_name

                    return_message = 'Receive Status Code: {} from {}'.format(response.status_code, url)
                    return "failure", "{}".format(return_message), None, pre_submit_name

                response = response.text
                resp = json.loads(response)

                if response_success_condition:

                    self.log.info('call pre submit ({}, {}) - validate success condition'.format(index, pre_submit_name))

                    # validate success condition
                    condition_name = response_success_condition.get('name') or None
                    condition_type = response_success_condition.get('type') or None
                    condition_value = response_success_condition.get('value') or None

                    if condition_name is not None and condition_type is not None and condition_value is not None:
                        value = self.getValue(condition_name, resp)
                        try:
                            condition_value = self.convertTypeData(condition_type, condition_value)
                        except Exception as exc:
                            self.log.debug("[Pre-submit] {} is failure: {}".format(pre_submit_name, str(exc)))

                            self.log.info('call pre submit ({}, {}) - Internal Error (Convert Success Condition Type)'.format(index, pre_submit_name))

                            return "failure", "Internal Error (Convert Success Condition Type): {}".format(str(exc)), None, pre_submit_name

                        if value != condition_value:
                            # failure
                            return_message = self.getValue(failure_response_message, resp) or 'API Failure'
                            
                            self.log.debug("[Pre-submit][{}] Not match Success".format(pre_submit_name))

                            self.log.info('call pre submit ({}, {}) - Not match Success: {}'.format(index, pre_submit_name, return_message))

                            return "failure", "{}".format(return_message), None, pre_submit_name


                # check valid
                if validity:

                    self.log.info('call pre submit ({}, {}) - check valid'.format(index, pre_submit_name))

                    if bypass_validation == False:
                        if valid_condition:
                            validity_name = valid_condition.get('name') or None
                            validity_type = valid_condition.get('type') or None
                            validity_value = valid_condition.get('value') or None

                            self.log.info('call pre submit ({}, {}) - check valid - validity_name: {}'.format(index, pre_submit_name, validity_name))
                            self.log.info('call pre submit ({}, {}) - check valid - validity_type: {}'.format(index, pre_submit_name, validity_type))
                            self.log.info('call pre submit ({}, {}) - check valid - validity_value: {}'.format(index, pre_submit_name, validity_value))
                        
                            if validity_name is not None and validity_type is not None and validity_value is not None:
                                value = self.getValue(validity_name, resp)

                                self.log.info('call pre submit ({}, {}) - check valid - value: {}'.format(index, pre_submit_name, value))

                                try:
                                    validity_value = self.convertTypeData(validity_type, validity_value)
                                except Exception as exc:
                                    self.log.debug("[Pre-submit] {} is invalid: {}".format(pre_submit_name, str(exc)))

                                    self.log.info('call pre submit ({}, {}) - Internal Error (Convert Valid Condition Type): {}'.format(index, pre_submit_name, exc))

                                    return "failure", "Internal Error (Convert Valid Condition Type): {}".format(str(exc)), None, pre_submit_name
                        
                                if value == validity_value:
                                    self.log.debug("[Pre-submit][{}] Valid".format(pre_submit_name))

                                    self.log.info('call pre submit ({}, {}) - validate valid'.format(index, pre_submit_name))

                                    # valid
                                    self.log.debug('PreSubmit.preSubmitVerify.index.before.mappingRequest.mapping_value: {}, {}'.format(index, mapping_value))
                                    self.log.debug('PreSubmit.preSubmitVerify.index.before.mappingRequest.request: {}, {}'.format(index, request))
                                    self.log.debug('PreSubmit.preSubmitVerify.index.before.mappingRequest.resp: {}, {}'.format(index, resp))

                                    self.log.info('call pre submit ({}, {}) - mappingRequest - mapping_value: {}'.format(index, pre_submit_name, mapping_value))
                                    self.log.info('call pre submit ({}, {}) - mappingRequest - request: {}'.format(index, pre_submit_name, request))
                                    self.log.info('call pre submit ({}, {}) - mappingRequest - resp: {}'.format(index, pre_submit_name, resp))

                                    request = self.mappingRequest(mapping_value, request, resp)
                                    self.log.debug('PreSubmit.preSubmitVerify.index.after.mappingRequest.request: {}, {}'.format(index, request))
                                    self.log.debug("[Pre-submit][{}] Mapping Request: {}".format(pre_submit_name, request))

                                    self.log.info('call pre submit ({}, {}) - mappingRequest - request: {}'.format(index, pre_submit_name, request))

                                    valid_message_property = message_property.get('valid') or None
                                    valid_message_format = message_property.get('fvalid') or None

                                    if valid_message_property is not None or valid_message_format is not None:
                                        valid_message = self.getValue(valid_message_property, resp) or ''
                                        if valid_message_format is not None:
                                            valid_message = valid_message_format.replace('${}', str(valid_message))

                                        valid_message = valid_message or 'valid'

                                        if not is_multiple:
                                            self.putPresubmit(sequence, request, True)
                                            return "success_with_message", valid_message, None, pre_submit_name
                                        
                                        if not ignore:
                                            return "success_with_message", valid_message, None, pre_submit_name
                                        elif ignore:
                                            continue

                                    continue

                        # check warning
                        if warning_condition:
                            
                            self.log.info('call pre submit ({}, {}) - check warning'.format(index, pre_submit_name))

                            warning_name = warning_condition.get('name') or None
                            warning_type = warning_condition.get('type') or None
                            warning_value = warning_condition.get('value') or None

                            if warning_name is not None and warning_type is not None and warning_value is not None:
                                value = self.getValue(warning_name, resp)
                                try:
                                    warning_value = self.convertTypeData(warning_type, warning_value)
                                except Exception as exc:
                                    self.log.debug("[Pre-submit] {} is invalid: {}".format(pre_submit_name, str(exc)))

                                    self.log.info('call pre submit ({}, {}) - Internal Error (Convert Warning Condition Type): {}'.format(index, pre_submit_name, exc))

                                    return "failure", "Internal Error (Convert Warning Condition Type): {}".format(str(exc)), None, pre_submit_name

                                if value == warning_value:
                                    # warning

                                    self.log.info('call pre submit ({}, {}) - mappingRequest - mapping_value: {}'.format(index, pre_submit_name, mapping_value))
                                    self.log.info('call pre submit ({}, {}) - mappingRequest - request: {}'.format(index, pre_submit_name, request))
                                    self.log.info('call pre submit ({}, {}) - mappingRequest - resp: {}'.format(index, pre_submit_name, resp))
                                    
                                    request = self.mappingRequest(mapping_value, request, resp)
                                        
                                    self.log.debug("[Pre-submit][{}] Mapping Request: {}".format(pre_submit_name, request))

                                    self.log.info('call pre submit ({}, {}) - mappingRequest - request: {}'.format(index, pre_submit_name, request))
                                    
                                    if not is_multiple:
                                        self.putPresubmit(sequence, request)

                                    warning_message_property = message_property.get('warning') or None
                                    warning_message_format = message_property.get('fwarning') or None
                                    warning_message = self.getValue(warning_message_property, resp) or ''

                                    if warning_message_format is not None:
                                        warning_message = warning_message_format.replace('${}', str(warning_message))

                                    warning_message = warning_message or 'warning'
                                    
                                    self.log.debug("[Pre-submit][{}] Warning: {}".format(pre_submit_name, warning_message))

                                    if not is_multiple:
                                        return "warning", "{}".format(warning_message), None, pre_submit_name

                                    if not ignore:
                                        return "warning", "{}".format(warning_message), None, pre_submit_name
                                    elif ignore:
                                        continue

                        # invalid case
                        invalid_message_property = message_property.get('invalid') or None
                        invalid_message_format = message_property.get('finvalid') or None
                        invalid_message = self.getValue(invalid_message_property, resp) or ''

                        if invalid_message_format is not None:
                            invalid_message = invalid_message_format.replace('${}', str(invalid_message))

                        invalid_message = invalid_message or 'invalid'

                        self.log.debug("[Pre-submit][{}] Invalid: {}".format(pre_submit_name, invalid_message))

                        return "failure", "{}".format(invalid_message), None, pre_submit_name
                    else:
                        request = self.mappingRequest(mapping_value, request, resp)
                        self.log.debug("[Pre-submit][{}] Mapping Request: {}".format(pre_submit_name, request))
                        continue
                else:
                    request = self.mappingRequest(mapping_value, request, resp)
                    self.log.debug("[Pre-submit][{}] Mapping Request: {}".format(pre_submit_name, request))
                    continue
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            exception_message = exc_type, fname, exc_tb.tb_lineno
            self.log.error('PreSubmit.preSubmitVerify.e: {}'.format(e))
            self.log.error('PreSubmit.preSubmitVerify.exception_message: {}'.format(exception_message))

        self.log.debug('PreSubmit.preSubmitVerify.request.out: {}'.format(request))

        # Don't have pre-submit          
        return "success", "valid", request, None
    
    def getInstance(self, current_reference_uuid, instanceList):
        for instance in instanceList:
            if current_reference_uuid == instance.get('reference_uuid'):
                return instance
                # sub_sequence = instance.get('sub_sequence') or 0
                # if sub_sequence > 0:
                #     selected_flag = instance.get('selected') or False
                #     if selected_flag == True:
                #         return instance
                # else:
                #     return instance

        return dict()
                            