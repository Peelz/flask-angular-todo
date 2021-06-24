import sys
import os

from app.commons import constants
from app.commons.abstract_flow_tracking_information import AbstractFlowTrackingInformation
from app.commons.getcache import GetcacheViews

class AdditionalCondition(AbstractFlowTrackingInformation):

    def __init__(self):
        try:
            self.getcache_views = GetcacheViews()
            super().__init__()
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            exception_message = exc_type, fname, exc_tb.tb_lineno
            self.logger.error('AdditionalCondition.__init__.e: {}'.format(e))
            self.logger.error('AdditionalCondition.__init__.exception_message: {}'.format(exception_message))

    def adapter(self, request_body):
        output = {}
        try:

            self.logger.debug('AdditionalCondition.adapter')

            dataset_name = request_body.get('dataset_name')
            field_name = request_body.get('field_name')
            sorting = request_body.get('sorting') or list()
            filter_condition = request_body.get('filter')
            options = request_body.get('options') or dict()

            self.logger.debug('AdditionalCondition.adapter.self.current_instance: {}'.format(self.current_instance))

            reference_additional_conditions = self.current_instance.get('reference_additional_condition') or list()

            self.logger.debug('AdditionalCondition.adapter.reference_additional_conditions: {}'.format(reference_additional_conditions))

            current_node_cache = self.getcache_views.post()
            self.logger.debug('AdditionalCondition.adapter.current_node_cache: {}'.format(current_node_cache))

            for reference_additional_condition in reference_additional_conditions:
                if dataset_name == reference_additional_condition.get('dataset_name') and field_name == reference_additional_condition.get('field_name'):
                    condition = reference_additional_condition.get('condition')
                    condition = self.cast_dynamic_additional(condition, current_node_cache)

                    if condition is not None and condition:
                        if filter_condition is None or not filter_condition:
                            filter_condition = "({})".format(condition)
                        else:
                            filter_condition += ' and ({})'.format(condition)

                    break
            
            output = {
                'field_name': field_name,
                'dataset_name': dataset_name,
                'sorting': sorting,
                'filter': filter_condition,
                'options': options,
            }

            self.logger.debug('AdditionalCondition.adapter.output: {}'.format(output))
        
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            exception_message = exc_type, fname, exc_tb.tb_lineno
            self.logger.error('AdditionalCondition.adapter.e: {}'.format(e))
            self.logger.error('AdditionalCondition.adapter.exception_message: {}'.format(exception_message))

        return output
