import redis
import datetime
import json
import os
import sys
import re

from flask import current_app as app
from flask import jsonify, request

from app.commons import constants
from app.commons.abstract_activity import AbstractActivity


class PutcacheViews(AbstractActivity):

    disable_extend = True


    def __init__(self):
        super().__init__()

    def convertTypeData(self, expectType, value):
        if value == None:
            return None
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


    def getValue(self, propertyName, body):
        property_name = self.convertObjectToAssociateArray(propertyName)

        try:
            value = eval('body' + property_name)
        except Exception:
            value = None
        
        return value


    def post(self):
        try:

            json_return = {
                "meta": {
                    "response_ref": self.reference_id,
                    "response_datetime": datetime.datetime.now().strftime("%d-%m-%Y %H:%M:%S"),
                    "response_code": "10000",
                    "response_desc": "success"
                }
            }

            if self.flow_tracking.get('appInfo') is not None:
                return jsonify({})

            current_reference_uuid = self.flow_tracking.get('current_reference_uuid')
            instances = self.flow_tracking.get('instances') or list()

            instance = self.getInstance(current_reference_uuid, instances)
            # sequence = instance.get('sequence') or 0
            # sub_sequence = instance.get('sub_sequence') or 0

            self.logger.debug('PutcacheViews.post.current_reference_uuid: {}'.format(current_reference_uuid))
            self.logger.debug('PutcacheViews.post.instances: {}'.format(instances))
            self.logger.debug('PutcacheViews.post.instance: {}'.format(instance))
            
            intent = instance.get('intent') or dict()
            intent_outs = intent.get('outs') or list()

            self.logger.debug('PutcacheViews.post.intent: {}'.format(intent))
            self.logger.debug('PutcacheViews.post.intent_outs: {}'.format(intent_outs))

            param = request.json.get('param', {})

            self.logger.debug('PutcacheViews.post.param: {}'.format(param))

            if isinstance(param, list):
                param = param[0]

            data = dict()
            flow_intent_alias_data = {}
            flow_intent_found_alias = []
            if intent_outs:
                for item in intent_outs:
                    intent_name = item.get('name')
                    intent_multiple = item.get('multiple')

                    value = None
                    if intent_name.endswith('_to'):
                        if intent_name in param:
                            value = self.getValue(intent_name, param)
                        else:
                            value = self.getValue(intent_name.replace('_to', ''), param)
                    else:
                        value = self.getValue(intent_name, param)

                    if intent_multiple == True:
                        result_list = list()
                        if not isinstance(value, list):
                            result_list.append(value)
                            value = result_list

                    intent_name = intent_name.replace('[]', '[0]')
                    data[intent_name] = value

                    self.logger.debug('flow_intent_alias.item ({}, {}): {}'.format(intent_name, value, item))

                    if 'ref_flow_intent_name' in item and item.get('ref_flow_intent_name'): 
                        flow_intent_alias_data[item['ref_flow_intent_name']] = {
                            'key': intent_name,
                            'value': value
                        }
                        flow_intent_found_alias.append(intent_name)
            
            self.logger.debug('flow_intent_alias.flow_intent_alias_data: {}'.format(flow_intent_alias_data))
            self.logger.debug('flow_intent_alias.flow_intent_found_alias: {}'.format(flow_intent_found_alias))

            # data['sequence'] = sequence
            # data['sub_sequence'] = sub_sequence
            data['reference_uuid'] = current_reference_uuid

            key = 'INTENT_' + self.workflowkey

            self.logger.debug('PutcacheViews.post.key: {}'.format(key))

            try:
                intent_cache = self.cache.get_json(key)
            except Exception:
                intent_cache = dict()
            
            self.logger.debug('PutcacheViews.post.intent_cache: {}'.format(intent_cache))
            
            if not intent_cache:
                intents = list()
                intents.append(data)
            else:
                isFound = False
                intents = intent_cache.get('intents') or list()

                self.logger.debug('PutcacheViews.post.intents: {}'.format(intents))
                self.logger.debug('PutcacheViews.post.current_reference_uuid: {}'.format(current_reference_uuid))

                for index, item in enumerate(intents):
                    reference_uuid_cacahe = item.get('reference_uuid')
                    if reference_uuid_cacahe == current_reference_uuid:
                        isFound = True
                        break
            
                if isFound == False:
                    intents.append(data)
                else:
                    intents[index] = data
                
                self.logger.debug('PutcacheViews.post.isFound: {}'.format(isFound))
                self.logger.debug('PutcacheViews.post.intents: {}'.format(intents))
                self.logger.debug('PutcacheViews.post.index: {}'.format(index))
                self.logger.debug('PutcacheViews.post.data: {}'.format(data))

            flow_intent_index = -1
            for index, item in enumerate(intents):
                if item['reference_uuid'] == 'flow_intent':
                    flow_intent_index = index
            
            self.logger.debug('PutcacheViews.post.flow_intent.flow_intent_index: {}'.format(flow_intent_index))
            
            current_intent_index = -1
            for index, item in enumerate(intents):
                if item['reference_uuid'] == current_reference_uuid:
                    current_intent_index = index
            
            self.logger.debug('PutcacheViews.post.flow_intent.current_intent_index: {}'.format(current_intent_index))
            
            self.logger.debug('PutcacheViews.post.flow_intent.before.intents: {}'.format(intents))

            if flow_intent_index >= 0 and current_intent_index >= 0:
                for c_key, c_value in intents[current_intent_index].items():
                    for flow_intent in self.flow_intents:
                        if c_key == flow_intent['name'] and c_key not in flow_intent_found_alias:
                            if flow_intent['multiple'] == True:
                                if isinstance(c_value, list):
                                    temps = []
                                    for item in c_value:
                                        item_value = self.convertTypeData(flow_intent['type'], item)
                                        temps.append(item_value)
                                    intents[flow_intent_index][c_key] = temps
                                else:
                                    item_value = self.convertTypeData(flow_intent['type'], c_value)
                                    intents[flow_intent_index][c_key] = [item_value]
                            else:
                                item_value = self.convertTypeData(flow_intent['type'], c_value)
                                intents[flow_intent_index][c_key] = item_value
            
            if flow_intent_index >= 0 and current_intent_index >= 0:
                for alias_name, obj in flow_intent_alias_data.items():
                    for flow_intent in self.flow_intents:
                        if alias_name == flow_intent['name']:
                            if flow_intent['multiple'] == True:
                                if isinstance(obj.get('value'), list):
                                    temps = []
                                    for item in obj.get('value'):
                                        item_value = self.convertTypeData(flow_intent['type'], item)
                                        temps.append(item_value)
                                    intents[flow_intent_index][alias_name] = temps
                                else:
                                    item_value = self.convertTypeData(flow_intent['type'], obj.get('value'))
                                    intents[flow_intent_index][alias_name] = [item_value]
                            else:
                                item_value = self.convertTypeData(flow_intent['type'], obj.get('value'))
                                intents[flow_intent_index][alias_name] = item_value
                                    
            self.logger.debug('PutcacheViews.post.flow_intent.after.intents: {}'.format(intents))

            data = {
                'intents': intents,
            }

            key = 'INTENT_' + self.workflowkey
            self.cache.put_json(key, data)

            self.logger.debug("Put Cache Response {}: {}".format(self.workflowkey, data))
            
            return jsonify(json_return)
                
        except Exception as exc:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            exception_message = exc_type, fname, exc_tb.tb_lineno
            self.logger.error('PutcacheViews.post.exc: {}'.format(exc))
            self.logger.error('PutcacheViews.post.exception_message: {}'.format(exception_message))

            raise Exception(exc)
