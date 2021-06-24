from datetime import datetime

from app.commons import constants
from app.commons.abstract_flow_tracking_information import AbstractFlowTrackingInformation
from app.commons.connectors.launcher import Launcher
from app.commons.getcache import GetcacheViews

class CreateTransformation(AbstractFlowTrackingInformation):

    def adapter(self, request_body, logger):
        try:
            business_keys = list()
            if 'business_key' in request_body:
                business_keys = request_body.pop('business_key') or list()
                    
                for business_key in business_keys:
                    name = business_key.get('name')
                    field_name = business_key.get('field')
                    field_type = business_key.get('type')

                    request_data = dict()
                    request_data['data'] = dict()
                    request_data['data']['key'] = name
                    
                    service_name = 'business_gen_key'
                    response_data = Launcher(logger).service_manager(service_name, request_data)

                    msg_code = response_data.get('meta', dict()).get('response_code')
                    if msg_code != '20000':
                        raise Exception('Business Key Gen Error: {}'.format(response_data.get('meta', dict()).get('response_desc')))

                    business = response_data.get('data', dict()).get('businesskeyID') or None
                    if business is None:
                        raise Exception('Business Key Gen ID is None')

                    if field_type == 'text':
                        request_body['request_data'][0][field_name] = str(business)
                    else:
                        request_body['request_data'][0][field_name] = int(business)

            request_data = request_body.get('request_data', list())
            record_status = request_data[0].get(constants.FIELD_RECORD_STATUS)
            dataset_name = request_body.get('dataset_name')
            field_name = request_body.get('field_name')

            if record_status is None:
                default_creations = self.current_instance.get('reference_default_creation') or list()
                creation_record_status = 2

                for default_creation in default_creations:
                    if default_creation.get('dataset_name') == dataset_name and default_creation.get('field_name') == field_name:
                        creation_record_status = default_creation.get('default_record_status')
                        if creation_record_status is None:
                            creation_record_status = 2

                request_body['request_data'][0][constants.FIELD_RECORD_STATUS] = creation_record_status
            
            request_body['request_data'][0][constants.FIELD_ACTION_DATE] = int(datetime.now().timestamp()) * 1000
            request_body['request_data'][0][constants.FIELD_ACTION_ID] = self.workflowkey
            request_body['request_data'][0][constants.FIELD_ACTION_BY] = self.username
            request_body['request_data'][0][constants.FIELD_CREATE_USER] = self.username
            request_body['request_data'][0][constants.FIELD_CREATE_DATE] = int(datetime.now().timestamp()) * 1000
            
            if dataset_name != '1_2652':
                if self.app_code is not None:
                    request_body['request_data'][0][constants.FIELD_EXECUTION_APPLICATION] = int(self.app_code)
            
            if self.branch_information is not None:
                request_body['request_data'][0][constants.FIELD_EXECUTION_LOCATION] = str(self.branch_information)
            
            # if getattr(constants, 'DATA_PRIVACY', False):
            overwrite_dp = None
            overwrite_dc = None
            overwrite_sub = None
            current_node_cache = GetcacheViews().post()
            self.logger.debug('CreateTransformation.DATA_PRIVACY.current_node_cache: {}'.format(current_node_cache))
            if constants.FIELD_DATA_PROCESSOR in current_node_cache and current_node_cache[constants.FIELD_DATA_PROCESSOR]:
                overwrite_dp = current_node_cache[constants.FIELD_DATA_PROCESSOR]
            if constants.FIELD_DATA_CONTROLLER in current_node_cache and current_node_cache[constants.FIELD_DATA_CONTROLLER]:
                overwrite_dc = current_node_cache[constants.FIELD_DATA_CONTROLLER]
            if constants.FIELD_SUB_CONTROLLER in current_node_cache and current_node_cache[constants.FIELD_SUB_CONTROLLER]:
                overwrite_sub = current_node_cache[constants.FIELD_SUB_CONTROLLER]
            self.logger.debug('CreateTransformation.DATA_PRIVACY.overwrite_dp: {}'.format(overwrite_dp))
            self.logger.debug('CreateTransformation.DATA_PRIVACY.overwrite_dc: {}'.format(overwrite_dc))
            self.logger.debug('CreateTransformation.DATA_PRIVACY.overwrite_sub: {}'.format(overwrite_sub))
            if self.data_processor is not None and self.data_processor:
                request_body['request_data'][0][constants.FIELD_DATA_PROCESSOR] = str(self.data_processor)
            if overwrite_dp is not None and overwrite_dp:
                request_body['request_data'][0][constants.FIELD_DATA_PROCESSOR] = str(overwrite_dp)
            if self.data_controller is not None and self.data_controller:
                request_body['request_data'][0][constants.FIELD_DATA_CONTROLLER] = str(self.data_controller)
            if overwrite_dc is not None and overwrite_dc:
                request_body['request_data'][0][constants.FIELD_DATA_CONTROLLER] = str(overwrite_dc)
            if self.sub_controller is not None and self.sub_controller:
                request_body['request_data'][0][constants.FIELD_SUB_CONTROLLER] = str(self.sub_controller)
            if overwrite_sub is not None and overwrite_sub:
                request_body['request_data'][0][constants.FIELD_SUB_CONTROLLER] = str(overwrite_sub)

            request_body['request_data'][0][constants.FIELD_THIRD_PARTY] = None

            return request_body
            
        except Exception as exc:
            raise exc


            
        
        

