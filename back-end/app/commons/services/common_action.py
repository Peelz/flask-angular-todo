import sys
import os
import uuid
import json

from flask import session

from datetime import datetime
from copy import deepcopy

from app.commons import constants
from app.commons.abstract_flow_tracking_information import AbstractFlowTrackingInformation
from app.commons.services.cache import Cache
from app.commons.connectors.launcher import Launcher
from app.commons.services.pre_submit import PreSubmit
from app.commons.getcache import GetcacheViews
from app.commons.utils.tool import Tool

class CommonAction(AbstractFlowTrackingInformation):

    def __init__(self, logger):
        super().__init__()
        self.logger = logger
        self.logger.debug('CommonAction.__init__')
        self.logger.info('init call common action')
        self.reference_id = session['reference_id']
        self.logger.info('call common action - flow_tracking: {}'.format(self.flow_tracking))
    
    def pre_search_transaction(self, launcher, request_data_multi):
        try:
            self.logger.debug('CommonAction.pre_search_transaction.request_data_multi.in: {}'.format(request_data_multi))

            self.logger.info('call common action pre_search_transaction - request_data_multi (in): {}'.format(request_data_multi))

            for row in request_data_multi.get('transaction_request_data', []):
                query = row['request_data']['query']
                query_splits = query.split(' ')
                self.logger.debug('CommonAction.pre_search_transaction.query_splits: {}'.format(query_splits))
                found_index_from = -1
                for index, value in enumerate(query_splits):
                    if value.strip().lower() == 'from':
                        found_index_from = index
                        break
                self.logger.debug('CommonAction.pre_search_transaction.found_index_from: {}'.format(found_index_from))
                found_index_where = -1
                for index, value in enumerate(query_splits):
                    if value.strip().lower() == 'where':
                        found_index_where = index
                        break
                self.logger.debug('CommonAction.pre_search_transaction.found_index_where: {}'.format(found_index_where))
                dataset_name = ''
                if found_index_from != -1:
                    dataset_name = query_splits[found_index_from + 1].strip()
                self.logger.debug('CommonAction.pre_search_transaction.dataset_name: {}'.format(dataset_name))
                condition = ''
                if found_index_where != -1:
                    condition = ' '.join(query_splits[(found_index_where + 1):])
                query_without_condition = ' '.join(query_splits[:found_index_where])
                self.logger.debug('CommonAction.pre_search_transaction.condition: {}'.format(condition))
                self.logger.debug('CommonAction.pre_search_transaction.query_without_condition: {}'.format(query_without_condition))
                self.logger.debug('CommonAction.pre_search_transaction.condition.data_privacy.before: {}'.format(condition))
                condition = self.data_privacy_condition_2(launcher, dataset_name, condition, True)
                self.logger.debug('CommonAction.pre_search_transaction.condition.data_privacy.after: {}'.format(condition))
                if condition:
                    row['request_data']['query'] = '{} where {}'.format(query_without_condition, condition)
                else:
                    row['request_data']['query'] = query_without_condition
            
            self.logger.info('call common action pre_search_transaction - request_data_multi (out): {}'.format(request_data_multi))

            self.logger.debug('CommonAction.pre_search_transaction.request_data_multi.out: {}'.format(request_data_multi))
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            exception_message = exc_type, fname, exc_tb.tb_lineno
            self.logger.error('CommonAction.pre_search_transaction.e: {}'.format(e))
            self.logger.error('CommonAction.pre_search_transaction.exception_message: {}'.format(exception_message))
            raise e
        return request_data_multi

    def pre_search(self, request_data, override_condition=None):
        try:
            self.logger.debug('CommonAction.pre_search.request_data: {}'.format(request_data))
            self.logger.debug('CommonAction.pre_search.override_condition: {}'.format(override_condition))

            if 'dataset_name' in request_data:
                dataset_name = request_data.get('dataset_name')
                request_data.pop('dataset_name')
            else:
                dataset_name = getattr(constants, 'DATASET_NAME', None)
            if dataset_name is None:
                raise Exception('dataset_name is required')
            
            self.logger.info('call common action pre_search ({}) - dataset_name: {}'.format(dataset_name, dataset_name))
            self.logger.info('call common action pre_search ({}) - request_data: {}'.format(dataset_name, request_data))
            self.logger.info('call common action pre_search ({}) - override_condition: {}'.format(dataset_name, override_condition))

            data_field = request_data.get('fields') or dict()
            data_field = self.get_login_info(data_field)
            need_verify = request_data.get('need_verify') or False
            options = request_data.get('options') or dict()
            sorting = request_data.get('sorting') or dict()

            sorting_override = ''

            template_default_sorting = request_data.pop('template_default_sorting', False)

            self.logger.debug('CommonAction.pre_search.self.template_default_sorting: {}'.format(template_default_sorting))
            
            self.logger.info('call common action pre_search ({}) - template_default_sorting: {}'.format(dataset_name, template_default_sorting))

            # construct condition
            condition = override_condition if override_condition is not None else self.tql_build_condition(data_field)
            if getattr(constants, 'INQUIRY_DATA_FILTER', None):
                additional = constants.INQUIRY_DATA_FILTER
                idx_cut = additional.find("order by")
                
                if idx_cut != -1:
                    additional_condition = additional[:idx_cut].strip()
                    sorting_override = additional[idx_cut:]
                else:
                    additional_condition = additional

                if additional_condition.strip() != "":
                    if condition is None or not condition:
                        condition = "({})".format(additional_condition)
                    else:
                        condition += ' and ({})'.format(additional_condition)

            self.logger.debug('CommonAction.pre_search.self.current_instance: {}'.format(self.current_instance))

            additional_inquiry_condition =  self.current_instance.get('additional_inquiry_condition') or list()

            self.logger.debug('CommonAction.pre_search.additional_inquiry_condition: {}'.format(additional_inquiry_condition))

            self.logger.info('call common action pre_search ({}) - additional_inquiry_condition: {}'.format(dataset_name, additional_inquiry_condition))

            if additional_inquiry_condition:
                additional_inquiry = additional_inquiry_condition
                idx_cut = additional_inquiry.find("order by")

                if idx_cut != -1:
                    additional_inquiry_condition = additional_inquiry[:idx_cut].strip()
                    sorting_override = additional_inquiry[idx_cut:]
                    self.logger.debug('CommonAction.pre_search.sorting_override: {}'.format(sorting_override))
                else:
                    additional_inquiry_condition = additional_inquiry_condition

                if additional_inquiry_condition.strip() != "":
                    current_node_cache = GetcacheViews().post()
                    additional_inquiry_condition = self.cast_dynamic_additional(additional_inquiry_condition, current_node_cache)
                    
                    if condition is None or not condition:
                        condition = '({})'.format(additional_inquiry_condition)
                    else:
                        condition += ' and ({})'.format(additional_inquiry_condition)

            self.logger.debug('CommonAction.pre_search.condition.data_privacy.before: {}'.format(condition))
            condition = self.data_privacy_condition(dataset_name, condition)
            self.logger.debug('CommonAction.pre_search.condition.data_privacy.after: {}'.format(condition))

            self.logger.info('call common action pre_search ({}) - data_privacy_condition: {}'.format(dataset_name, condition))

            # use default sortin in this function
            sorting = self.tql_build_sorting(sorting)

            self.logger.info('call common action pre_search ({}) - sorting: {}'.format(dataset_name, sorting))

            queries = 'select * from {}'.format(dataset_name)
            if condition is not None or not condition:
                queries += ' where {}'.format(condition)

            if template_default_sorting:
                self.logger.debug('CommonAction.pre_search.case[1][template_default_sorting]')
                if sorting_override == '':
                    queries += ' order by {}'.format(sorting)
                    self.logger.debug('CommonAction.pre_search.case[1.1][template_default_sorting]')
                else:
                    queries += ' {}'.format(sorting_override)
                    self.logger.debug('CommonAction.pre_search.case[1.2][sorting_override]')
            else:
                self.logger.debug('CommonAction.pre_search.case[2]')
                if sorting is not None and sorting != '':
                    queries += ' order by {}'.format(sorting)
                    self.logger.debug('CommonAction.pre_search.case[2.1][from table sort]')
                elif sorting_override != '':
                    queries += ' {}'.format(sorting_override)
                    self.logger.debug('CommonAction.pre_search.case[2.2][sorting_override]')

            query_request = dict()
            query_request['request_data'] = dict()
            query_request['request_data']['query'] = queries
            query_request['request_data']['$options'] = options

            self.logger.info('call common action pre_search ({}) - query_request: {}'.format(dataset_name, query_request))

            return query_request

        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            exception_message = exc_type, fname, exc_tb.tb_lineno
            self.logger.error('CommonAction.pre_search.e: {}'.format(e))
            self.logger.error('CommonAction.pre_search.exception_message: {}'.format(exception_message))
            raise e

    # main activities
    def search(self, service_name, request_data, override_condition=None):
        self.logger.info('start call common action search')
        try:

            self.logger.debug('CommonAction.search.service_name: {}'.format(service_name))
            self.logger.debug('CommonAction.search.request_data: {}'.format(request_data))
            self.logger.debug('CommonAction.search.override_condition: {}'.format(override_condition))

            self.logger.info('call common action search ({}) - service_name: {}'.format(service_name, service_name))
            self.logger.info('call common action search ({}) - request_data: {}'.format(service_name, request_data))
            self.logger.info('call common action search ({}) - override_condition: {}'.format(service_name, override_condition))

            query_request = self.pre_search(request_data, override_condition)

            self.logger.info('call common action search ({}) - query_request: {}'.format(service_name, query_request))

            self.logger.debug('CommonAction.search.query_request: {}'.format(query_request))
            response = Launcher(self.logger).service_manager(service_name, query_request)
            self.logger.debug('CommonAction.search.response: {}'.format(response))
            try:
                self.logger.debug('CommonAction.search.response.json(): {}'.format(response.json()))
            except Exception as e:
                self.logger.error('CommonAction.search.response cannot .json()')

            self.logger.info('end call common action search')

            return response
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            exception_message = exc_type, fname, exc_tb.tb_lineno
            self.logger.error('CommonAction.search.e: {}'.format(e))
            self.logger.error('CommonAction.search.exception_message: {}'.format(exception_message))
            raise e

    def create(self, service_name, request_data):
        self.logger.info('start call common action create')
        try:
            self.logger.debug('CommonAction.create.service_name: {}'.format(service_name))
            self.logger.debug('CommonAction.create.request_data: {}'.format(request_data))
            if 'dataset_name' in request_data:
                dataset_name = request_data.get('dataset_name')
                request_data.pop('dataset_name')
            else:
                dataset_name = getattr(constants, 'DATASET_NAME', None)
            
            if dataset_name is None:
                raise Exception('dataset_name is required')
            
            ignore_warning_message_combine = False
            if 'ignore_warning_message_combine' in request_data:
                ignore_warning_message_combine = request_data.get('ignore_warning_message_combine', False)
                request_data.pop('ignore_warning_message_combine')

            self.logger.info('call common action create ({}) - dataset_name: {}'.format(dataset_name, dataset_name))
            self.logger.info('call common action create ({}) - service_name: {}'.format(dataset_name, service_name))
            self.logger.info('call common action create ({}) - request_data: {}'.format(dataset_name, request_data))

            create_request = dict()
            create_request['dataset_name'] = dataset_name
            create_request['partial_commit'] = False

            data_field = request_data.get('fields') or dict()
            data_field = self.get_login_info(data_field)
            if isinstance(data_field, list):
                for i in range(len(data_field)):
                    data = data_field[i]
                    data = self.clean_dummy_field(deepcopy(data))
                    data = self.set_data_param(data)
                    data = self.set_data_owner(dataset_name, data)
                    data = self.remove_not_in_meta_field(dataset_name, data)
                    data[constants.FIELD_PROCESS_STATUS] = constants.PROCESS_CREATE
                    data_field[i] = data
                create_request['request_data'] = data_field
            else:
                data_field = self.clean_dummy_field(deepcopy(data_field))
                data_field = self.set_data_param(data_field)
                data_field = self.set_data_owner(dataset_name, data_field)
                data_field = self.remove_not_in_meta_field(dataset_name, data_field)
                data_field[constants.FIELD_PROCESS_STATUS] = constants.PROCESS_CREATE
                create_request['request_data'] = [data_field]
            
            self.logger.info('call common action create ({}) - create_request: {}'.format(dataset_name, create_request))

            # ToDo pre-submit
            self.logger.debug('CommonAction.create.create_request: {}'.format(create_request))
            request_data_temp_ins = create_request['request_data']
            request_data_temp_outs = []
            handle_error_indexs = []
            self.logger.debug('CommonAction.create.request_data_temp_ins: {}'.format(request_data_temp_ins))
            for index, request_data_temp_in in enumerate(request_data_temp_ins):
                create_request['request_data'] = [request_data_temp_in]
                pre_submit_response = self.call_pre_submit(create_request, ignore_warning_message_combine, len(request_data_temp_ins) > 1)
                self.logger.debug('CommonAction.create.pre_submit_response ({}): {}'.format(index, pre_submit_response))
                if pre_submit_response['is_success']:
                    self.logger.debug('CommonAction.create.success')
                    self.logger.info('call common action create ({}) - pre_submit_response success')
                    create_request = pre_submit_response['data']
                    request_data_temp_outs.append(create_request['request_data'][0])
                else:
                    self.logger.debug('CommonAction.create.fail')
                    self.logger.info('call common action create ({}) - pre_submit_response fail')
                    pre_submit_response['data']['error_index'] = index
                    self.logger.debug('CommonAction.create.pre_submit_response.error: {}'.format(pre_submit_response))
                    if len(request_data_temp_ins) > 1:
                        self.logger.debug('CommonAction.create.push.handle_error_indexs')
                        handle_error_indexs.append({
                            'index': index,
                            'data': json.loads(json.dumps(pre_submit_response)),
                        })
                    else:
                        return pre_submit_response['data']
                if index == len(request_data_temp_ins) - 1:
                    self.logger.debug('CommonAction.create.handle_error_indexs: {}'.format(handle_error_indexs))
                    if len(handle_error_indexs) > 0:
                        pre_submit_response['data']['handle_error_indexs'] = handle_error_indexs
                        self.logger.debug('CommonAction.create.pre_submit_response.data: {}'.format(pre_submit_response['data']))
                        return pre_submit_response['data']
            self.logger.debug('CommonAction.create.request_data_temp_outs: {}'.format(request_data_temp_outs))
            create_request['request_data'] = request_data_temp_outs

            self.logger.info('call common action create ({}) - service_name: {}'.format(service_name, service_name))
            self.logger.info('call common action create ({}) - create_request: {}'.format(service_name, create_request))

            self.logger.debug('CommonAction.create.create_request: {}'.format(create_request))

            self.logger.info('call common action create ({}) - service_name: {}'.format(dataset_name, service_name))
            self.logger.info('call common action create ({}) - create_request: {}'.format(dataset_name, create_request))

            response = Launcher(self.logger).service_manager(service_name, create_request)
            
            self.logger.debug('CommonAction.create.response: {}'.format(response))

            self.logger.info('call common action create ({}) - response: {}'.format(dataset_name, response))

            try:
                self.logger.debug('CommonAction.create.response.json(): {}'.format(response.json()))

                response_json = response.json()
                response_json_log = Tool.remove_sensitive_key_for_logging(response_json)

                self.logger.info('call common action create ({}) - response_json_log: {}'.format(service_name, response_json_log))
            except Exception as e:
                self.logger.error('CommonAction.create.response cannot .json()')
            if getattr(constants, 'PARAMETER_DATASET', False):
                if response and response.json() and response.json()['response_data']:
                    for data in response.json()['response_data']:
                        self.clear_cache(dataset_name, data)

            # ToDo send notification

            self.logger.info('end call common action create')

            return response

        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            exception_message = exc_type, fname, exc_tb.tb_lineno
            self.logger.error('CommonAction.create.e: {}'.format(e))
            self.logger.error('CommonAction.create.exception_message: {}'.format(exception_message))
            raise e


    def update(self, service_name, request_data):
        self.logger.info('start call common action update')
        try:
            self.logger.debug('CommonAction.update.service_name: {}'.format(service_name))
            self.logger.debug('CommonAction.update.request_data: {}'.format(request_data))
            if 'dataset_name' in request_data:
                dataset_name = request_data.get('dataset_name')
                request_data.pop('dataset_name')
            else:
                dataset_name = getattr(constants, 'DATASET_NAME', None)

            if dataset_name is None:
                raise Exception('dataset_name is required')
            
            ignore_warning_message_combine = False
            if 'ignore_warning_message_combine' in request_data:
                ignore_warning_message_combine = request_data.get('ignore_warning_message_combine', False)
                request_data.pop('ignore_warning_message_combine')

            self.logger.info('call common action update ({}) - dataset_name: {}'.format(dataset_name, dataset_name))
            self.logger.info('call common action update ({}) - service_name: {}'.format(dataset_name, service_name))
            self.logger.info('call common action update ({}) - request_data: {}'.format(dataset_name, request_data))

            update_request = dict()
            update_request['dataset_name'] = dataset_name

            data_field = request_data.get('fields') or dict()
            data_field = self.get_login_info(data_field)

            self.logger.debug('CommonAction.update.data_field : {}'.format(data_field))

            if isinstance(data_field, list):
                for i in range(len(data_field)):
                    data = data_field[i]
                    data = self.clean_dummy_field(deepcopy(data))
                    data = self.set_data_param(data)
                    data = self.remove_not_in_meta_field(dataset_name, data)
                    data = self.remove_dc_and_dp_field(data)
                    if constants.FIELD_PROCESS_STATUS not in data or data.get(constants.FIELD_PROCESS_STATUS, None) == constants.PROCESS_CREATE:
                        data[constants.FIELD_PROCESS_STATUS] = constants.PROCESS_UPDATE
                    
                    for field, value in data.items():
                        if not value and type(value) is list:
                            data[field] = None
                    
                    if constants.FIELD_LAST_UPDATE in data:
                        data.pop(constants.FIELD_LAST_UPDATE)

                    if request_data.get('is_verify') or False:
                        if constants.FIELD_RECORD_STATUS in data and constants.STATUS_WAIT_FOR_VERIFY == data[constants.FIELD_RECORD_STATUS]:
                            raise Exception('Record status field is wait for verify')
                    
                    data_field[i] = data
                update_request['request_data'] = data_field
            else:
                data_field = self.clean_dummy_field(deepcopy(data_field))
                data_field = self.set_data_param(data_field)
                data_field = self.remove_not_in_meta_field(dataset_name, data_field)
                data_field = self.remove_dc_and_dp_field(data_field)
                if constants.FIELD_PROCESS_STATUS not in data_field or data_field.get(constants.FIELD_PROCESS_STATUS, None) == constants.PROCESS_CREATE:
                    data_field[constants.FIELD_PROCESS_STATUS] = constants.PROCESS_UPDATE
            
                for field, value in data_field.items():
                    if not value and type(value) is list:
                        data_field[field] = None
                
                if constants.FIELD_LAST_UPDATE in data_field:
                    data_field.pop(constants.FIELD_LAST_UPDATE)

                if request_data.get('is_verify') or False:
                    if constants.FIELD_RECORD_STATUS in data_field and constants.STATUS_WAIT_FOR_VERIFY == data_field[constants.FIELD_RECORD_STATUS]:
                        raise Exception('Record status field is wait for verify')

                update_request['request_data'] = [data_field]
            
            self.logger.info('call common action update ({}) - update_request: {}'.format(dataset_name, update_request))
            
            # ToDo pre-submit
            self.logger.debug('CommonAction.update.update_request: {}'.format(update_request))
            request_data_temp_ins = update_request['request_data']
            request_data_temp_outs = []
            handle_error_indexs = []
            self.logger.debug('CommonAction.update.request_data_temp_ins: {}'.format(request_data_temp_ins))
            for index, request_data_temp_in in enumerate(request_data_temp_ins):
                update_request['request_data'] = [request_data_temp_in]
                pre_submit_response = self.call_pre_submit(update_request, ignore_warning_message_combine, len(request_data_temp_ins) > 1)
                self.logger.debug('CommonAction.update.pre_submit_response ({}): {}'.format(index, pre_submit_response))
                if pre_submit_response['is_success']:
                    self.logger.debug('CommonAction.update.success')
                    update_request = pre_submit_response['data']
                    request_data_temp_outs.append(update_request['request_data'][0])
                else:
                    self.logger.debug('CommonAction.update.fail')
                    pre_submit_response['data']['error_index'] = index
                    self.logger.debug('CommonAction.update.pre_submit_response.error: {}'.format(pre_submit_response))
                    if len(request_data_temp_ins) > 1:
                        self.logger.debug('CommonAction.update.push.handle_error_indexs')
                        handle_error_indexs.append({
                            'index': index,
                            'data': json.loads(json.dumps(pre_submit_response)),
                        })
                    else:
                        return pre_submit_response['data']
                if index == len(request_data_temp_ins) - 1:
                    self.logger.debug('CommonAction.update.handle_error_indexs: {}'.format(handle_error_indexs))
                    if len(handle_error_indexs) > 0:
                        pre_submit_response['data']['handle_error_indexs'] = handle_error_indexs
                        self.logger.debug('CommonAction.update.pre_submit_response.data: {}'.format(pre_submit_response['data']))
                        return pre_submit_response['data']
            self.logger.debug('CommonAction.update.request_data_temp_outs: {}'.format(request_data_temp_outs))
            update_request['request_data'] = request_data_temp_outs

            self.logger.debug('CommonAction.update.update_request: {}'.format(update_request))

            self.logger.info('call common action update ({}) - service_name: {}'.format(dataset_name, service_name))
            self.logger.info('call common action update ({}) - update_request: {}'.format(dataset_name, update_request))

            response = Launcher(self.logger).service_manager(service_name, update_request)

            self.logger.debug('CommonAction.update.response: {}'.format(response))

            self.logger.info('call common action update ({}) - response: {}'.format(dataset_name, response))

            try:
                self.logger.debug('CommonAction.update.response.json(): {}'.format(response.json()))
                response_json = response.json()
                response_json_log = Tool.remove_sensitive_key_for_logging(response_json)
                self.logger.info('call common action update ({}) - response_json_log: {}'.format(dataset_name, response_json_log))
            except Exception as e:
                self.logger.error('CommonAction.update.response cannot .json()')
            if getattr(constants, 'PARAMETER_DATASET', False):
                if response and response.json() and response.json()['response_data']:
                    for data in response.json()['response_data']:
                        self.clear_cache(dataset_name, data)

            # ToDo send notification

            self.logger.info('end call common action update')

            return response

        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            exception_message = exc_type, fname, exc_tb.tb_lineno
            self.logger.error('CommonAction.update.e: {}'.format(e))
            self.logger.error('CommonAction.update.exception_message: {}'.format(exception_message))
            raise e


    def delete(self, service_name, request_data):
        self.logger.info('start call common action delete')
        try:
            self.logger.debug('CommonAction.delete.service_name: {}'.format(service_name))
            self.logger.debug('CommonAction.delete.request_data: {}'.format(request_data))
            if 'dataset_name' in request_data:
                dataset_name = request_data.get('dataset_name')
                request_data.pop('dataset_name')
            else:
                dataset_name = getattr(constants, 'DATASET_NAME', None)

            if dataset_name is None:
                raise Exception('dataset_name is required')
            
            ignore_warning_message_combine = False
            if 'ignore_warning_message_combine' in request_data:
                ignore_warning_message_combine = request_data.get('ignore_warning_message_combine', False)
                request_data.pop('ignore_warning_message_combine')

            self.logger.info('call common action delete ({}) - dataset_name: {}'.format(dataset_name, dataset_name))
            self.logger.info('call common action delete ({}) - service_name: {}'.format(dataset_name, service_name))
            self.logger.info('call common action delete ({}) - request_data: {}'.format(dataset_name, request_data))

            update_request = dict()
            update_request['dataset_name'] = dataset_name

            data_field = request_data.get('fields') or dict()
            data_field = self.get_login_info(data_field)

            self.logger.debug('CommonAction.delete.data_field : {}'.format(data_field))

            if isinstance(data_field, list):
                for i in range(len(data_field)):
                    data = data_field[i]
                    data = self.clean_dummy_field(deepcopy(data))
                    data = self.set_data_param(data)
                    data = self.remove_not_in_meta_field(dataset_name, data)
                    data = self.remove_dc_and_dp_field(data)
                    data = self.set_status_canceled(data)
                    if constants.FIELD_PROCESS_STATUS not in data:
                        data[constants.FIELD_PROCESS_STATUS] = constants.PROCESS_DELETE
                    
                    for field, value in data.items():
                        if not value and type(value) is list:
                            data[field] = None
                    
                    if constants.FIELD_LAST_UPDATE in data:
                        data.pop(constants.FIELD_LAST_UPDATE)

                    if request_data.get('is_verify') or False:
                        if constants.FIELD_RECORD_STATUS in data and constants.STATUS_WAIT_FOR_VERIFY == data[constants.FIELD_RECORD_STATUS]:
                            raise Exception('Record status field is wait for verify')
                    
                    data_field[i] = data
                update_request['request_data'] = data_field
            else:
                data_field = self.clean_dummy_field(deepcopy(data_field))
                data_field = self.set_data_param(data_field)
                data_field = self.remove_not_in_meta_field(dataset_name, data_field)
                data_field = self.remove_dc_and_dp_field(data_field)
                data_field = self.set_status_canceled(data_field)
                if constants.FIELD_PROCESS_STATUS not in data_field:
                    data_field[constants.FIELD_PROCESS_STATUS] = constants.PROCESS_UPDATE
            
                for field, value in data_field.items():
                    if not value and type(value) is list:
                        data_field[field] = None
                
                if constants.FIELD_LAST_UPDATE in data_field:
                    data_field.pop(constants.FIELD_LAST_UPDATE)

                if request_data.get('is_verify') or False:
                    if constants.FIELD_RECORD_STATUS in data_field and constants.STATUS_WAIT_FOR_VERIFY == data_field[constants.FIELD_RECORD_STATUS]:
                        raise Exception('Record status field is wait for verify')

                update_request['request_data'] = [data_field]
            
            self.logger.info('call common action delete ({}) - update_request: {}'.format(dataset_name, update_request))

            self.logger.debug('CommonAction.delete.update_request: {}'.format(update_request))

            self.logger.info('call common action delete ({}) - service_name: {}'.format(dataset_name, service_name))
            self.logger.info('call common action delete ({}) - update_request: {}'.format(dataset_name, update_request))

            response = Launcher(self.logger).service_manager(service_name, update_request)

            self.logger.debug('CommonAction.delete.response: {}'.format(response))

            self.logger.info('call common action delete ({}) - response: {}'.format(dataset_name, response))

            try:
                self.logger.debug('CommonAction.delete.response.json(): {}'.format(response.json()))
                response_json = response.json()
                response_json_log = Tool.remove_sensitive_key_for_logging(response_json)
                self.logger.info('call common action delete ({}) - response_json_log: {}'.format(dataset_name, response_json_log))
            except Exception as e:
                self.logger.error('CommonAction.delete.response cannot .json()')
            if getattr(constants, 'PARAMETER_DATASET', False):
                if response and response.json() and response.json()['response_data']:
                    for data in response.json()['response_data']:
                        self.clear_cache(dataset_name, data)

            # ToDo send notification

            self.logger.info('end call common action delete')

            return response

        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            exception_message = exc_type, fname, exc_tb.tb_lineno
            self.logger.error('CommonAction.delete.e: {}'.format(e))
            self.logger.error('CommonAction.delete.exception_message: {}'.format(exception_message))
            raise e


    # set status
    def set_status_approve(self, data_field):
        data_field[constants.FIELD_RECORD_STATUS] = constants.STATUS_APPROVE
        data_field[constants.FIELD_VERIFY_DATE] = int(datetime.now().timestamp()) * 1000
        data_field[constants.FIELD_VERIFY_BY] = self.username


    def set_status_wait_for_verify(self, data_field):
        data_field[constants.FIELD_RECORD_STATUS] = constants.STATUS_WAIT_FOR_VERIFY
        data_field[constants.FIELD_CREATE_USER] = self.username
        data_field[constants.FIELD_CREATE_DATE] = int(datetime.now().timestamp()) * 1000
        if constants.FIELD_EFFECTIVE_DATE not in data_field:
            data_field[constants.FIELD_EFFECTIVE_DATE] = int(datetime.now().timestamp()) * 1000


    def set_status_wait_for_cancel(self, data_field):
        data_field[constants.FIELD_RECORD_STATUS] = constants.STATUS_WAIT_FOR_CANCEL
        data_field[constants.FIELD_CREATE_USER] = self.username
        data_field[constants.FIELD_CREATE_DATE] = int(datetime.now().timestamp()) * 1000
        if constants.FIELD_EFFECTIVE_DATE not in data_field:
            data_field[constants.FIELD_EFFECTIVE_DATE] = int(datetime.now().timestamp()) * 1000


    def set_status_canceled(self, data_field):
        data_field[constants.FIELD_RECORD_STATUS] = constants.STATUS_CANCELED
        data_field[constants.FIELD_CREATE_USER] = self.username
        data_field[constants.FIELD_CREATE_DATE] = int(datetime.now().timestamp()) * 1000
        if constants.FIELD_EFFECTIVE_DATE not in data_field:
            data_field[constants.FIELD_EFFECTIVE_DATE] = int(datetime.now().timestamp()) * 1000
        return data_field


    def set_status_active(self, data_field):
        data_field[constants.FIELD_RECORD_STATUS] = constants.STATUS_ACTIVE
        data_field[constants.FIELD_CREATE_USER] = self.username
        data_field[constants.FIELD_CREATE_DATE] = int(datetime.now().timestamp()) * 1000
        if constants.FIELD_EFFECTIVE_DATE not in data_field:
            data_field[constants.FIELD_EFFECTIVE_DATE] = int(datetime.now().timestamp()) * 1000


    def set_status_reject(self, data_field):
        data_field[constants.FIELD_RECORD_STATUS] = constants.STATUS_REJECT
        data_field[constants.FIELD_CREATE_USER] = self.username
        data_field[constants.FIELD_CREATE_DATE] = int(datetime.now().timestamp()) * 1000


    # set param
    def set_data_param(self, data_field):
        data_field[constants.FIELD_ACTION_DATE] = int(datetime.now().timestamp()) * 1000
        data_field[constants.FIELD_ACTION_ID] = self.workflowkey
        data_field[constants.FIELD_ACTION_BY] = self.username
        data_field[constants.FIELD_CREATE_USER] = self.username
        data_field[constants.FIELD_CREATE_DATE] = int(datetime.now().timestamp()) * 1000
        
        return data_field


    def set_data_owner(self, dataset_name, data_field):
        try:
            if dataset_name != '1_2652':
                if self.app_code is not None:
                    data_field[constants.FIELD_EXECUTION_APPLICATION] = int(self.app_code)

            if self.branch_information is not None:
                data_field[constants.FIELD_EXECUTION_LOCATION] = str(self.branch_information)
                
            # if getattr(constants, 'DATA_PRIVACY', False):
            overwrite_dp = None
            overwrite_dc = None
            overwrite_sub = None
            current_node_cache = GetcacheViews().post()
            self.logger.debug('CommonAction.set_data_owner.current_node_cache: {}'.format(current_node_cache))
            if constants.FIELD_DATA_PROCESSOR in current_node_cache and current_node_cache[constants.FIELD_DATA_PROCESSOR]:
                overwrite_dp = current_node_cache[constants.FIELD_DATA_PROCESSOR]
            if constants.FIELD_DATA_CONTROLLER in current_node_cache and current_node_cache[constants.FIELD_DATA_CONTROLLER]:
                overwrite_dc = current_node_cache[constants.FIELD_DATA_CONTROLLER]
            if constants.FIELD_SUB_CONTROLLER in current_node_cache and current_node_cache[constants.FIELD_SUB_CONTROLLER]:
                overwrite_sub = current_node_cache[constants.FIELD_SUB_CONTROLLER]
            
            self.logger.debug('CommonAction.set_data_owner.data_field[constants.FIELD_DATA_PROCESSOR]: {}'.format(data_field.get(constants.FIELD_DATA_PROCESSOR, None)))
            self.logger.debug('CommonAction.set_data_owner.self.data_processor: {}'.format(self.data_processor))
            self.logger.debug('CommonAction.set_data_owner.overwrite_dp: {}'.format(overwrite_dp))

            self.logger.debug('CommonAction.set_data_owner.data_field[constants.FIELD_DATA_CONTROLLER]: {}'.format(data_field.get(constants.FIELD_DATA_CONTROLLER, None)))
            self.logger.debug('CommonAction.set_data_owner.self.data_controller: {}'.format(self.data_controller))
            self.logger.debug('CommonAction.set_data_owner.overwrite_dc: {}'.format(overwrite_dc))

            self.logger.debug('CommonAction.set_data_owner.data_field[constants.FIELD_SUB_CONTROLLER]: {}'.format(data_field.get(constants.FIELD_SUB_CONTROLLER, None)))
            self.logger.debug('CommonAction.set_data_owner.self.sub_controller: {}'.format(self.sub_controller))
            self.logger.debug('CommonAction.set_data_owner.overwrite_sub: {}'.format(overwrite_sub))

            if self.data_processor is not None and self.data_processor and not data_field.get(constants.FIELD_DATA_PROCESSOR, None):
                data_field[constants.FIELD_DATA_PROCESSOR] = str(self.data_processor)
            if overwrite_dp is not None and overwrite_dp:
                data_field[constants.FIELD_DATA_PROCESSOR] = str(overwrite_dp)

            if self.data_controller is not None and self.data_controller and not data_field.get(constants.FIELD_DATA_CONTROLLER, None):
                data_field[constants.FIELD_DATA_CONTROLLER] = str(self.data_controller)
            if overwrite_dc is not None and overwrite_dc:
                data_field[constants.FIELD_DATA_CONTROLLER] = str(overwrite_dc)

            if self.sub_controller is not None and self.sub_controller and not data_field.get(constants.FIELD_SUB_CONTROLLER, None):
                data_field[constants.FIELD_SUB_CONTROLLER] = str(self.sub_controller)
            if overwrite_sub is not None and overwrite_sub:
                data_field[constants.FIELD_SUB_CONTROLLER] = str(overwrite_sub)

            data_field[constants.FIELD_THIRD_PARTY] = None
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            exception_message = exc_type, fname, exc_tb.tb_lineno
            self.logger.error('CommonAction.set_data_owner.e: {}'.format(e))
            self.logger.error('CommonAction.set_data_owner.exception_message: {}'.format(exception_message))

        return data_field

    
    # tql builder
    def tql_build_condition(self, data_field, reference_name=None, ignore_none=True):
        # filter out dummy field and value is null or empty
        data_field = { f:v for f,v in data_field.items() if 'dummy' not in f and ((ignore_none and v or v == 0) or not ignore_none) }
        condition = list()

        key = reference_name
        for field, value in data_field.items():
            reference_name = '{}.{}'.format(reference_name, field) if reference_name else field

            if type(value) is dict:
                if 'from' in value and 'to' in value:
                    value = { f:v for f,v in value.items() if v or v == 0 }

                    # for date between
                    for f, v in value.items():
                        if f == 'from':
                            condition.append('{} >= {}'.format(field, v))
                        elif f == 'to':
                            condition.append('{} <= {}'.format(field, v))
                
                else:
                    condition_element = self.tql_build_condition(value, field)
                    if condition_element:
                        condition.append(condition_element)

                key = None
                reference_name = None

            elif type(value) is list:
                value = list(map(lambda v: '"{}"'.format(v) if type(v) is str else str(v), value))
                if reference_name.startswith('NOT'):
                    _, field_name = reference_name.split('NOT', 1)
                    condition.append('{} not in ({})'.format(field_name, ','.join(value)))
                else:
                    condition.append('{} in ({})'.format(field, ','.join(value)))
                
                reference_name = key

            elif type(value) is str:
                for keyword in constants.SEARCH_OPERATOR_LIST:
                    if value.startswith(keyword):
                        _, value = value.split(keyword, 1)
                        condition.append('contains_ignore_case({},"{}")'.format(reference_name, value))
                        break
                else:
                    if reference_name == '_8738':
                        condition.append('{} in ("{}")'.format(reference_name, value))
                    elif reference_name.startswith('NOT'):
                        _, field_name = reference_name.split('NOT', 1)
                        condition.append('{} != "{}"'.format(field_name, value))
                    elif reference_name.startswith('IN'):
                        _, field_name = reference_name.split('IN', 1)
                        condition.append('{} IN ({})'.format(field_name, value))

                    elif reference_name == 'other_conditions':
                        condition.append(value)

                    else:
                        condition.append('{} = "{}"'.format(reference_name, value))

                reference_name = key

            elif type(value) is int and len(str(value)) == 13:
                date_time = datetime.fromtimestamp(value / 1e3)
                day = date_time.day
                month = date_time.month
                year = date_time.year
                from_date = datetime(year, month, day, 0, 0, 0).timestamp() * 1000
                to_date = datetime(year, month, day, 23, 59, 59).timestamp() * 1000

                condition.append('{} >= {} and {} <= {}'.format(reference_name, int(from_date), reference_name, int(to_date)))

                reference_name = key

            else:
                if(value is not None):
                    value = str(value).lower()
                    for keyword in constants.SEARCH_OPERATOR_LIST:
                        if value.startswith(keyword):
                            _, value = value.split(keyword, 1)
                            condition.append('contains_ignore_case({},{})'.format(reference_name, value))
                            break

                        else:
                            if reference_name:
                                if reference_name.startswith('NOT'):
                                    _, field_name = reference_name.split('NOT', 1)
                                    condition.append('{} != {}'.format(field_name, value))

                                else:
                                    condition.append('{} = {}'.format(reference_name, value))
                        reference_name = key
                else:
                    if reference_name:
                        if reference_name.startswith('NOT'):
                            _, field_name = reference_name.split('NOT', 1)
                            condition.append('{} != null'.format(field_name))
                        else:
                            condition.append('{} = null'.format(reference_name))

        return " and ".join(condition)


    def tql_build_sorting(self, sorting):
        # default_sorting = getattr(constants, 'DEFAULT_SORTING', dict())

        # if not sorting and (default_sorting and type(default_sorting) is dict):
        #     sorting = default_sorting

        if sorting and type(sorting) is dict:
            sorting_result = list()
            for field, direction in sorting.items():
                sorting_result.append('{} {}'.format(field, direction))

            return ','.join(sorting_result)

        return str()

    
    # other function
    def clean_dummy_field(self, data):
        if type(data) is dict:
            remove_fields = list()
            for field, value in data.items():
                if 'dummy' in str(field):
                    remove_fields.append(field)
                    continue
                self.clean_dummy_field(value)

            return self._remove_dummy_field(data, remove_fields)

        elif type(data) is list:
            for elem in data:
                self.clean_dummy_field(elem)
            
        return data
                

    def _remove_dummy_field(self, data, remove_fields):
        for remove_field in remove_fields:
            del data[remove_field]

        return data


    def clear_cache(self, dataset_name, response_data):
        if getattr(constants, 'PARAMETER_DATASET', False):
            cache_api = Cache()
            if response_data.get('msg_code') == '30000':
                pattern = 'PARAMETER_{};*'.format(dataset_name)
                key_list = cache_api.list_key_matching(pattern)

                for key in key_list:
                    cache_api.delete(key)

    def remove_dc_and_dp_field(self, data_field):
        if constants.FIELD_DATA_PROCESSOR in data_field:
            data_field.pop(constants.FIELD_DATA_PROCESSOR)
        if constants.FIELD_DATA_CONTROLLER in data_field:
            data_field.pop(constants.FIELD_DATA_CONTROLLER)
        return data_field
    
    def call_pre_submit(self, request_data, ignore_warning_message_combine, is_multiple):
        output = {
            'is_success': False,
            'data': None
        }

        self.logger.debug('CommonAction.call_pre_submit.workflowkey: {}'.format(self.workflowkey))
        self.logger.debug('CommonAction.call_pre_submit.reference_id: {}'.format(self.reference_id))

        presubmit = PreSubmit(self.workflowkey, self.flow_tracking, self.reference_id)

        self.logger.debug('CommonAction.call_pre_submit.presubmit.update_request: {}'.format(request_data))

        self.logger.info('call common action call_pre_submit - request_data: {}'.format(request_data))
        self.logger.info('call common action call_pre_submit - ignore_warning_message_combine: {}'.format(ignore_warning_message_combine))

        result, message, override_body, pre_submit_name = presubmit.preSubmitVerify(request_data, ignore_warning_message_combine, is_multiple)

        self.logger.debug('CommonAction.call_pre_submit.presubmit.result: {}'.format(result))
        self.logger.debug('CommonAction.call_pre_submit.presubmit.message: {}'.format(message))
        self.logger.debug('CommonAction.call_pre_submit.presubmit.override_body: {}'.format(override_body))
        self.logger.debug('CommonAction.call_pre_submit.presubmit.pre_submit_name: {}'.format(pre_submit_name))

        self.logger.info('call common action call_pre_submit - result: {}'.format(result))
        self.logger.info('call common action call_pre_submit - message: {}'.format(message))
        self.logger.info('call common action call_pre_submit - override_body: {}'.format(override_body))
        self.logger.info('call common action call_pre_submit - pre_submit_name: {}'.format(pre_submit_name))

        result_return_list = ['failure', 'warning', 'failure and clear cache success', 'success_with_message']
        if result in result_return_list:
            response_format = {
                "meta": {
                    "response_ref": "",
                    "response_datetime": "",
                    "response_code": "",
                    "response_desc": "",
                    "service_code": ""
                },
                "data": {}
            }
            response_format['meta']['response_ref'] = self.reference_id
            response_format['meta']['response_datetime'] = datetime.now().strftime("%d-%m-%Y %H:%M:%S")
            response_format['meta']['response_desc'] = message
            response_format['meta']['service_code'] = pre_submit_name
            if result == 'success_with_message':
                response_format['meta']['response_code'] = '10200'
            elif result == 'failure':
                response_format['meta']['response_code'] = '50300'
            elif result == 'warning':
                response_format['meta']['response_code'] = '10300'
            else:
                response_format['meta']['response_code'] = '30300'
            output = {
                'is_success': False,
                'data': response_format
            }
        else:
            if override_body is not None:
                request_data = override_body
                self.logger.debug("Override Body: {}".format(request_data))
            output = {
                'is_success': True,
                'data': request_data
            }
        self.logger.debug('CommonAction.call_pre_submit.presubmit.output: {}'.format(output))
        self.logger.info('call common action call_pre_submit - output: {}'.format(output))
        return output
