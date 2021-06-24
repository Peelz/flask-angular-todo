import sys
import os
import redis
import json

from flask import request, jsonify
from flask import current_app as app
from datetime import datetime
from flask_api import status

from app.commons import constants
from app.commons.abstract_activity import AbstractActivity
from app.commons.connectors.launcher import Launcher

class SearchViews(AbstractActivity):

    def post(self):
        try:
            group_type = getattr(constants, 'GROUP_TYPE', None)
            journal_key = getattr(constants, 'JOURNAL', None).get('key') if type(getattr(constants, 'JOURNAL', None)) is dict else None
            table_type = getattr(constants, 'TABLE_TYPE', None).get('code') if type(getattr(constants, 'TABLE_TYPE', None)) is dict else None
            embeded_fields = getattr(constants, 'EMBEDED_FIELDS', list())
            standard_process_type = getattr(constants, 'STANDARD_PROCESS_TYPE', None)

            request_data = request.json.get('data')
            self.logger.debug('o2.request_data: {}'.format(request_data))
            self.logger.info('o2.request_data: {}'.format(request_data))

            if request_data is None:
                raise Exception('request is wrong format')

            data_field = request_data.get('fields') or dict()
            options = request_data.get('options') or dict()
            need_verify = request_data.get('need_verify') or False

            is_embed_public = options.get('embed_public', False)

            if 'embed_public' not in options:
                request_data['options']['embed_public'] = True

            if request_data['options']['embed_public']:
                if 'embed' not in request_data['options']:
                    request_data['options']['embed'] = embeded_fields

            service_name = 'data_query_normal_with_tql'
            if need_verify:
                if group_type == constants.GROUP_TYPE_WORKING:
                    service_name = 'data_query_journal_for_verified'
                else:
                    service_name = 'data_query_master_with_tql'

                request_data['options']['period'] = journal_key if journal_key else 1
                request_data['options']['status_field_name'] = constants.FIELD_RECORD_STATUS
                request_data['options']['status_wait_for_approve_value'] = constants.STATUS_WAIT_FOR_VERIFY
                request_data['options']['status_active_value'] = constants.STATUS_ACTIVE
                request_data['options']['status_reject_value'] = constants.status_reject_value
                
            else:
                 if table_type == constants.TABLE_TYPE_SINGLE_JOURNAL:
                    service_name = 'data_query_journal_with_tql'

            self.logger.debug('o2.service_name: {}'.format(service_name))
            self.logger.info('o2.service_name: {}'.format(service_name))

            if standard_process_type == 'CONTROL':
                data_field['fields'][constants.FIELD_RECORD_STATUS] = constants.STATUS_WAIT_FOR_VERIFY

            if group_type == constants.GROUP_TYPE_WORKING:
                request_data['options']['mapping'] = True

            result = self.common_action.search(service_name, request_data)
            
            result = self.virtual_dataset_2(result, is_embed_public)

            response = self.ui_response.get_response_for_de(result)

        except Exception as e:
            response = self.ui_response.error(str(e), self.reference_id)

        finally:
            return jsonify(response), status.HTTP_200_OK
    
    def virtual_dataset(self, response, is_embed_public):
        try:
            reference_master_dataset = getattr(constants, 'REFERENCE_MASTER_DATASET', [])
            self.logger.debug('CommonAction.virtual_dataset.reference_master_dataset: {}'.format(reference_master_dataset))
            self.logger.info('CommonAction.virtual_dataset.reference_master_dataset: {}'.format(reference_master_dataset))
            if reference_master_dataset and len(reference_master_dataset) > 0:
                response = response.json()
                response_data = response.get('response_data', {})
                self.logger.debug('CommonAction.virtual_dataset.response_data.in: {}'.format(response_data))
                index = 0
                for row in response_data:
                    for item in reference_master_dataset:
                        meta_fields = self.getMetaData(item.get('dataset_name'))
                        self.logger.debug('index_{}.CommonAction.virtual_dataset.meta_fields: {}'.format(index, meta_fields))
                        self.logger.info('index_{}.CommonAction.virtual_dataset.meta_fields: {}'.format(index, meta_fields))
                        fields = list(filter(lambda x: x.get('name') == item.get('field_name'), meta_fields))
                        self.logger.debug('index_{}.CommonAction.virtual_dataset.fields: {}'.format(index, fields))
                        self.logger.info('index_{}.CommonAction.virtual_dataset.fields: {}'.format(index, fields))
                        field = fields[0] if len(fields) > 0 else {}
                        self.logger.debug('index_{}.CommonAction.virtual_dataset.field: {}'.format(index, field))
                        self.logger.info('index_{}.CommonAction.virtual_dataset.field: {}'.format(index, field))
                        value_field = '"{}"'.format(row.get(item.get('main_field_name'))) if field.get('type') == 'text' else row.get(item.get('main_field_name'))
                        self.logger.debug('index_{}.CommonAction.virtual_dataset.value_field: {}'.format(index, value_field))
                        self.logger.info('index_{}.CommonAction.virtual_dataset.value_field: {}'.format(index, value_field))
                        param = {
                            'request_data': {
                                'query': ' select {}, max(_80002) from {} where {} = {} group by {} '.format(
                                    item.get('field_name'), 
                                    item.get('dataset_name'), 
                                    item.get('field_name'), 
                                    value_field, 
                                    item.get('field_name')
                                ),
                                '$options': {
                                    'embed_public': False
                                }
                            }
                        }
                        self.logger.debug('index_{}.CommonAction.virtual_dataset.1.max.param: {}'.format(index, param))
                        self.logger.info('index_{}.CommonAction.virtual_dataset.1.max.param: {}'.format(index, param))
                        result = Launcher(self.logger).service_manager('data_query_journal_with_tql', param)
                        self.logger.debug('index_{}.CommonAction.virtual_dataset.1.max.result: {}'.format(index, result))
                        condition = ''
                        if result.ok:
                            result = result.json()
                            self.logger.debug('index_{}.CommonAction.virtual_dataset.1.max.result.json(): {}'.format(index, result))
                            if result.get('msg_code') == '30000':
                                condition = '{} = {} and _80002 = {}'.format(
                                    item.get('field_name'), 
                                    '"{}"'.format(result.get('response_data')[0].get(item.get('field_name'))) if field.get('type') == 'text' else result.get('response_data')[0].get(item.get('field_name')), 
                                    result.get('response_data')[0].get('_80002_max')
                                )
                        self.logger.debug('index_{}.CommonAction.virtual_dataset.condition: {}'.format(index, condition))
                        self.logger.info('index_{}.CommonAction.virtual_dataset.condition: {}'.format(index, condition))
                        param = {
                            'request_data': {
                                'query': ' select * from {} where {} = {} {}'.format(
                                    item.get('dataset_name'), 
                                    item.get('field_name'), 
                                    value_field, 
                                    'and {}'.format(condition) if condition else ''
                                ),
                                '$options': {
                                    'embed_public': is_embed_public
                                }
                            }
                        }
                        self.logger.debug('index_{}.CommonAction.virtual_dataset.2.getData.param: {}'.format(index, param))
                        self.logger.info('index_{}.CommonAction.virtual_dataset.2.getData.param: {}'.format(index, param))
                        result = Launcher(self.logger).service_manager('data_query_journal_with_tql', param)
                        self.logger.debug('index_{}.CommonAction.virtual_dataset.2.getData.result: {}'.format(index, result))
                        if result.ok:
                            result = result.json()
                            self.logger.debug('index_{}.CommonAction.virtual_dataset.2.getData.result.json(): {}'.format(index, result))
                            if result.get('msg_code') == '30000':
                                for key, value in result.get('response_data')[0].items():
                                    if key not in row:
                                        row[key] = value
                    index = index + 1
                self.logger.debug('CommonAction.virtual_dataset.response_data.out: {}'.format(response_data))
            return response
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            exception_message = exc_type, fname, exc_tb.tb_lineno
            self.logger.error('CommonAction.virtual_dataset.e: {}'.format(e))
            self.logger.error('CommonAction.virtual_dataset.exception_message: {}'.format(exception_message))
            raise e
    
    def virtual_dataset_2(self, response, is_embed_public):
        try:
            reference_master_dataset = getattr(constants, 'REFERENCE_MASTER_DATASET', [])
            self.logger.debug('CommonAction.virtual_dataset_2.reference_master_dataset: {}'.format(reference_master_dataset))
            self.logger.info('CommonAction.virtual_dataset_2.reference_master_dataset: {}'.format(reference_master_dataset))
            if reference_master_dataset and len(reference_master_dataset) > 0:
                response = response.json()
                self.logger.debug('CommonAction.virtual_dataset_2.response.in: {}'.format(response))
                response_data = response.get('response_data', {})
                self.logger.debug('CommonAction.virtual_dataset_2.response_data: {}'.format(response_data))
                temp_data = {}
                for item in reference_master_dataset:
                    meta_fields = self.getMetaData(item.get('dataset_name'))
                    self.logger.debug('CommonAction.virtual_dataset_2.meta_fields: {}'.format(meta_fields))
                    self.logger.info('CommonAction.virtual_dataset_2.meta_fields: {}'.format(meta_fields))
                    fields = list(filter(lambda x: x.get('name') == item.get('field_name'), meta_fields))
                    self.logger.debug('CommonAction.virtual_dataset_2.fields: {}'.format(fields))
                    self.logger.info('CommonAction.virtual_dataset_2.fields: {}'.format(fields))
                    field = fields[0] if len(fields) > 0 else {}
                    self.logger.debug('CommonAction.virtual_dataset_2.field: {}'.format(field))
                    self.logger.info('CommonAction.virtual_dataset_2.field: {}'.format(field))
                    condition = ''
                    for row in response_data:
                        if condition:
                            condition += ', '
                        condition += '"{}"'.format(row.get(item.get('main_field_name'))) if field.get('type') == 'text' else row.get(item.get('main_field_name'))
                    condition = '({})'.format(condition)
                    param = {
                        'request_data': {
                            'query': ' select * from {} where {} in {} '.format(
                                item.get('dataset_name'), 
                                item.get('main_field_name'), 
                                condition
                            ),
                            '$options': {
                                'embed_public': is_embed_public
                            }
                        }
                    }
                    self.logger.debug('CommonAction.virtual_dataset_2.param: {}'.format(param))
                    self.logger.info('CommonAction.virtual_dataset_2.param: {}'.format(param))
                    result = Launcher(self.logger).service_manager('data_query_journal_with_tql', param)
                    self.logger.debug('CommonAction.virtual_dataset_2.result: {}'.format(result))
                    if result.ok:
                        result = result.json()
                        self.logger.debug('CommonAction.virtual_dataset_2.result: {}'.format(result))
                        if result.get('msg_code') == '30000':
                            temp_data[item.get('dataset_name')] = []
                            for row in result.get('response_data'):
                                found_obj = None
                                found_index = -1
                                for index, temp_data_item in enumerate(temp_data[item.get('dataset_name')]):
                                    if temp_data_item[item.get('main_field_name')] == row[item.get('main_field_name')]:
                                        found_obj = temp_data_item
                                        found_index = index
                                        break
                                if found_obj and found_index != -1:
                                    if row['_80002'] > found_obj['_80002']:
                                        temp_data.get(item.get('dataset_name'))[found_index] = row
                                else:
                                    temp_data.get(item.get('dataset_name')).append(row)
                self.logger.debug('CommonAction.virtual_dataset_2.temp_data: {}'.format(temp_data))
                for row in response_data:
                    for item in reference_master_dataset:
                        for item2 in temp_data.get(item.get('dataset_name'), []):
                            if row.get(item.get('main_field_name')) == item2.get(item.get('field_name')):
                                for key, value in item2.items():
                                    if key not in row:
                                        row[key] = value
                                break
            self.logger.debug('CommonAction.virtual_dataset_2.response.out: {}'.format(response))
            return response
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            exception_message = exc_type, fname, exc_tb.tb_lineno
            self.logger.error('CommonAction.virtual_dataset_2.e: {}'.format(e))
            self.logger.error('CommonAction.virtual_dataset_2.exception_message: {}'.format(exception_message))
            raise e

    def getMetaData(self, datasetName, descriptionDataset=None): 
        fieldList = None
        # urls = "https://api-v1-service.c1-alpha-tiscogroup.com/public/data-v2-service/dep-api/GetMetaData?client_id=7d54f79fb17946678775205aab308619&client_secret=530d2e1111174443BFB01891E40BD091"
        if(datasetName):
            data = {"dataset_name": datasetName}
            resp = Launcher(self.logger).service_manager('data_get_metadata', data, None)
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
        return fieldList
   