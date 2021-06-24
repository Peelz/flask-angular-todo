from app.commons import constants
from app.commons.abstract_flow_tracking_information import AbstractFlowTrackingInformation
import re

class SearchTransformation(AbstractFlowTrackingInformation):

    def adapter(self, request_body):
        dataset_name = request_body.get('dataset_name')
        field_name = request_body.get('field_name')
        sorting = request_body.get('sorting') or list()
        options = request_body.get('options') or dict()
        filter_condition = request_body.get('filter')
        filter_condition = filter_condition.strip() if filter_condition is not None else filter_condition
        sorting_override = ''

        if dataset_name is None:
            raise Exception('dataset_name is required')

        query_command = "select * from {}".format(dataset_name)

        if getattr(constants, 'DATASET_NAME', None) == dataset_name:
            if getattr(constants, 'INQUIRY_DATA_FILTER', False):
                additional = constants.INQUIRY_DATA_FILTER
                idx_cut = additional.find("order by")
                if idx_cut != -1:
                    additional_condition = additional[:idx_cut].strip()
                    sorting_override = additional[idx_cut:]
                else:
                    additional_condition = additional
                
                if additional_condition.strip() != "":
                    if filter_condition is None or not filter_condition:
                        filter_condition = "({})".format(additional_condition)
                    else:
                        filter_condition += ' and ({})'.format(additional_condition)

        if filter_condition:
            query_command += " where {}".format(filter_condition)
        
        foundOrderBy = re.search(r'order\s*by', query_command, re.IGNORECASE)
        if not foundOrderBy:
            if sorting and type(sorting) is list:
                # filter out field is None
                sorting = list(filter(lambda s: s.get('field'), sorting))
                sorting = list(map(lambda s: '{} {}'.format(s.get('field'), s.get('direction') or 'asc'), sorting))
                sorting = ','.join(sorting)

                query_command += " order by {}".format(sorting)
            elif sorting_override != '':
                query_command += " {}".format(sorting_override)
            elif sorting and type(sorting) is str and not sorting.strip().startswith("order by"):
                raise Exception("invalid")

        return {
            'request_data': { 
                'query': query_command,
                '$options': options,
             },
        }




       
        
        

