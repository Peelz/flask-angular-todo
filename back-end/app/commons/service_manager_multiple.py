import sys
import os
import json
import time
import redis
import collections
import re

from datetime import datetime
from flask_api import status
from flask import current_app as app
from flask import request, jsonify

from app.commons import constants
from app.commons.adapters.additional_condition import AdditionalCondition
from app.commons.adapters.latest_update_date import LatestUpdateDate
from app.commons.adapters.data_privacy import DataPrivacy
from app.commons.adapters.search_transformation import SearchTransformation
from app.commons.adapters.create_transformation import CreateTransformation
from app.commons.adapters.update_transformation import UpdateTransformation
from app.commons.abstract_activity import AbstractActivity
from app.commons.connectors.launcher import Launcher
from app.commons.service_manager import ServiceManagerViews
from threading import Thread

class ServiceManagerMultipleViews(AbstractActivity):

    def __init__(self):
        try:
            self.service_manager_views = ServiceManagerViews()
            super().__init__()
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            exception_message = exc_type, fname, exc_tb.tb_lineno
            self.logger.error('ServiceManagerMultipleViews.__init__.e: {}'.format(e))
            self.logger.error('ServiceManagerMultipleViews.__init__.exception_message: {}'.format(exception_message))

    def post(self):
        self.logger.debug('ServiceManagerMultipleViews.post.start')
        try:
            output = {}
            def process(app, inputs, output):
                with app.app_context(), app.test_request_context():
                    for body in inputs:
                        uuid = body.get('uuid')
                        request_data = {
                            'json': body
                        }
                        self.logger.debug('ServiceManagerMultipleViews.post.process.request_data ({}): {}'.format(uuid, request_data))
                        response_data = self.service_manager_views.process(request_data)
                        self.logger.debug('ServiceManagerMultipleViews.post.process.response_data ({}): {}'.format(uuid, response_data))
                        output[uuid] = response_data
            threads = []
            maximum_per_thread = 30
            start_index = 0
            end_index = start_index + maximum_per_thread
            bodys = request.json.get('body')
            if end_index > len(bodys):
                end_index = len(bodys)
            self.logger.debug('ServiceManagerMultipleViews.post.bodys: {}'.format(bodys))
            self.logger.debug('ServiceManagerMultipleViews.post.start_index.end_index: {}, {}'.format(start_index, end_index))
            self.logger.debug('ServiceManagerMultipleViews.post.len(bodys): {}'.format(len(bodys)))
            while end_index <= len(bodys):
                if end_index > len(bodys):
                    end_index = len(bodys)
                inputs = bodys[start_index:end_index]
                self.logger.debug('ServiceManagerMultipleViews.post.start_index.end_index.inputs ({}, {}): {}'.format(start_index, end_index, inputs))
                thread = Thread(target=process, args=(app._get_current_object(), inputs, output))
                thread.start()
                threads.append(thread)
                start_index = end_index - 1
                end_index = start_index + maximum_per_thread
            for thread in threads:
                thread.join()
            self.logger.debug('ServiceManagerMultipleViews.post.output: {}'.format(output))
            response = self.ui_response.success('success', self.reference_id, output)
            self.logger.debug('ServiceManagerMultipleViews.post.response: {}'.format(response))
            return jsonify(response)
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            exception_message = exc_type, fname, exc_tb.tb_lineno
            self.logger.error('ServiceManagerMultipleViews.post.e: {}'.format(e))
            self.logger.error('ServiceManagerMultipleViews.post.exception_message: {}'.format(exception_message))
            response = self.ui_response.error(str(e), self.reference_id)
            return jsonify(response)

