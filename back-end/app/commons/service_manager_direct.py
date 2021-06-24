import sys
import os
import json
import time
import redis
import collections
import re
import uuid
import requests

from datetime import datetime
from flask_api import status
from flask import current_app as app
from flask import request, jsonify, session

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

from app.commons.jaeger_util import JaegerUtil

class ServiceManagerDirectViews(AbstractActivity):

    def __init__(self):
        try:
            super().__init__()
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            exception_message = exc_type, fname, exc_tb.tb_lineno
            self.logger.error('ServiceManagerDirectViews.__init__.e: {}'.format(e))
            self.logger.error('ServiceManagerDirectViews.__init__.exception_message: {}'.format(exception_message))

    def post(self):
        self.logger.debug('ServiceManagerDirectViews.post.start')
        service_name = request.json.get('service_name')
        key = 'SERVICE_URL_{}'.format(service_name.upper())
        url = app.config.get(key)
        headers = dict()
        mule_client_id = app.config.get('MULE_CLIENT_ID')
        mule_client_secret = app.config.get('MULE_CLIENT_SECRET')
        flow_tracking = self.login_data.get_flow_tracking_with_wfk(self.workflowkey)
        login_info = flow_tracking.get('login_info')
        user_id = login_info.get('user_id')
        username = login_info.get('username')
        appid = int(login_info.get('app_id'))
        log_session_id = login_info.get('log_session_id') or str(uuid.uuid4())
        app_meta = {
            'user_id': user_id,
            'user_name': username,
            'state': app.config.get('CURRENT_STATE'),
            'sub_state': app.config.get('CURRENT_SUB_STATE'),
            'request_datetime': datetime.now().strftime("%d-%m-%Y %H:%M:%S"),
            'log_session_id': session.get('reference_id'),
            'app_no': str(appid),
            'flow_session_id': self.workflowkey,
        }
        headers = {
            'content-Type': 'application/json',
            'app-meta': json.dumps(app_meta),
            'client_id': mule_client_id,
            'client_secret': mule_client_secret,
        }
        method = request.json.get('method')
        body = request.json.get('body')
        body = self.get_login_info(body)
        cafile = app.config.get('SSL_CERT_FILE', '')
        self.logger.info('call service_manager_direct ({}, {}) - request service - url: {}'.format(service_name, method, url))
        self.logger.info('call service_manager_direct ({}, {}) - request service - headers: {}'.format(service_name, method, headers))
        self.logger.info('call service_manager_direct ({}, {}) - request service - body: {}'.format(service_name, method, body))
        output = None
        try:
            if JaegerUtil.tracer:
                with JaegerUtil.tracer.start_active_span(url) as scope:
                    if scope and scope.span:
                        scope.span.log_kv({'event': 'service_manager_direct'})
                        JaegerUtil.active_span = scope.span
                        headers = JaegerUtil.inject_header(None, url, method, headers)
                        if method == 'POST':
                            response = requests.post(url, headers=headers, data=json.dumps(body))
                        elif method == 'GET':
                            response = requests.get(url, headers=headers)
                        elif method == 'PUT':
                            response = requests.put(url, headers=headers, data=json.dumps(body))
                        elif method == 'DELETE':
                            response = requests.delete(url, headers=headers, data=json.dumps(body))
                        else:
                            raise Exception("Method [{}] not support".format(method))
                        self.logger.debug('ServiceManagerDirectViews.response.text: {}'.format(response.text))
                        output = response.json()
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            exception_message = exc_type, fname, exc_tb.tb_lineno
            self.logger.error('ServiceManagerDirectViews.post.e: {}'.format(e))
            self.logger.error('ServiceManagerDirectViews.post.exception_message: {}'.format(exception_message))
            response = self.ui_response.error(str(e), self.reference_id)
            output = response
        
        self.logger.info('call service_manager_direct ({}, {}) - request service - response: {}'.format(service_name, method, output))

        return jsonify(output)

