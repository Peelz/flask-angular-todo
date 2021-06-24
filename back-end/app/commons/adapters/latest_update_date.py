
from app.commons import constants
from app.commons.abstract_flow_tracking_information import AbstractFlowTrackingInformation


class LatestUpdateDate(AbstractFlowTrackingInformation):

    def adapter(self, request_body):
        if 'group_key_fields' in request_body:
            group_key_fields = request_body.pop('group_key_fields') or list()

            if group_key_fields:
                if request_body['sorting'] is None:
                    request_body['sorting'] = []

                request_body['sorting'].append({
                    'field': constants.FIELD_CREATE_DATE,
                    'direction': 'desc'
                })

        return request_body
