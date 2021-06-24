
from app.commons import constants
from app.commons.abstract_flow_tracking_information import AbstractFlowTrackingInformation
from app.commons.services.cache import Cache
from app.commons.getcache import GetcacheViews
from app.commons.connectors.launcher import Launcher

class DataPrivacy(AbstractFlowTrackingInformation):

    def adapter(self, request_body):
        dataset_name = request_body.get('dataset_name')
        field_name = request_body.get('field_name')
        sorting = request_body.get('sorting') or list()
        filter_condition = request_body.get('filter')
        options = request_body.get('options') or dict()

        self.logger.debug('DataPrivacy.adapter.filter_condition.data_privacy.dataset_name: {}'.format(dataset_name))
        self.logger.debug('DataPrivacy.adapter.filter_condition.data_privacy.field_name: {}'.format(field_name))
        self.logger.debug('DataPrivacy.adapter.filter_condition.data_privacy.sorting: {}'.format(sorting))
        self.logger.debug('DataPrivacy.adapter.filter_condition.data_privacy.filter_condition: {}'.format(filter_condition))
        self.logger.debug('DataPrivacy.adapter.filter_condition.data_privacy.options: {}'.format(options))

        self.logger.info('call data privacy ({}) - dataset_name: {}'.format(dataset_name, dataset_name))
        self.logger.info('call data privacy ({}) - field_name: {}'.format(dataset_name, field_name))
        self.logger.info('call data privacy ({}) - sorting: {}'.format(dataset_name, sorting))
        self.logger.info('call data privacy ({}) - filter_condition: {}'.format(dataset_name, filter_condition))
        self.logger.info('call data privacy ({}) - options: {}'.format(dataset_name, options))

        dataset_constant = getattr(constants, 'DATA_PRIVACY_DATASETS', list())
        self.logger.debug('DataPrivacy.adapter.filter_condition.data_privacy.constants.DATA_PRIVACY_DATASETS: {}'.format(dataset_constant))

        self.logger.info('call data privacy ({}) - dataset_constant: {}'.format(dataset_name, dataset_constant))
        # if dataset_name not in dataset_constant:
        #     return request_body

        self.logger.debug('DataPrivacy.adapter.filter_condition.data_privacy.before: {}'.format(filter_condition))
        
        filter_condition = self.data_privacy_condition(dataset_name, filter_condition)
        
        self.logger.debug('DataPrivacy.adapter.filter_condition.data_privacy.after: {}'.format(filter_condition))

        return {
            'field_name': field_name,
            'dataset_name': dataset_name,
            'sorting': sorting,
            'filter': filter_condition,
            'options': options,
        }
