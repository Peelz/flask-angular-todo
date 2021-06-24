from datetime import datetime

from app.commons import constants
from app.commons.abstract_flow_tracking_information import AbstractFlowTrackingInformation


class UpdateTransformation(AbstractFlowTrackingInformation):

    def adapter(self, request_body):
        request_data = request_body.get('request_data') or list()
        data_field = request_data[0]
        record_link_id = data_field.get('record_link_id')

        for field, value in data_field.items():
            if not value and type(value) is list:
                request_body['request_data'][0][field] = None

        if constants.FIELD_LAST_UPDATE in data_field:
            del request_body['request_data'][0][constants.FIELD_LAST_UPDATE]

        if constants.FIELD_ID in data_field:
            del request_body['request_data'][0][constants.FIELD_ID]


        request_body['request_data'][0]['$tql_cond'] = "{} = \"{}\" ".format("record_link_id", record_link_id)
        request_body['request_data'][0][constants.FIELD_PROCESS_STATUS] = constants.PROCESS_UPDATE
        request_body['request_data'][0][constants.FIELD_ACTION_DATE] = int(datetime.now().timestamp()) * 1000
        request_body['request_data'][0][constants.FIELD_ACTION_ID] = self.workflowkey
        request_body['request_data'][0][constants.FIELD_ACTION_BY] = self.username
        request_body['request_data'][0][constants.FIELD_CREATE_USER] = self.username
        request_body['request_data'][0][constants.FIELD_CREATE_DATE] = int(datetime.now().timestamp()) * 1000

        return request_body
