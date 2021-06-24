import json
import time
import uuid
import os
import sys

from datetime import datetime
from functools import wraps, update_wrapper
from flask import current_app as app
from flask import Flask, render_template, jsonify, Blueprint, redirect, request, Response, send_from_directory, session
from flask import make_response
from app.commons import constants

from app.commons.configuration import ConfigurationViews
from app.commons.service_manager import ServiceManagerViews
from app.commons.service_manager_direct import ServiceManagerDirectViews
from app.commons.service_manager_multiple import ServiceManagerMultipleViews
from app.commons.getcache import GetcacheViews
from app.commons.putcache import PutcacheViews

from app.activities.search import SearchViews

from app.commons.services.log import Logger
from app.commons.services.cache import Cache

from app.commons.connectors.launcher import Launcher
from app.commons.login_data import LoginData

import time
import requests

from app.commons.jaeger_util import JaegerUtil

from app.commons.request_wrapping import RequestWrapping

def no_cache(f):
    def new_func(*args, **kwargs):
        resp = make_response(f(*args, **kwargs))
        resp.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, post-check=0, pre-check=0, max-age=0'
        resp.headers['Pragma'] = 'no-cache'
        resp.headers['Expires'] = '-1'
        return resp
    return update_wrapper(new_func, f)

def generate_session():
    reference_id = str(uuid.uuid4())
    wfk = request.headers.get('wfk')
    if not wfk:
        wfk = request.args.get('wfk')
    session['wfk'] = wfk
    flow_tracking = LoginData().get_flow_tracking()
    login_info = flow_tracking.get('login_info', {})
    code = flow_tracking.get('code')
    current_reference_uuid = flow_tracking.get('current_reference_uuid')
    user_id = login_info.get('user_id')
    app_id = login_info.get('app_id')
    session['reference_id'] = reference_id
    session['code'] = code
    session['current_reference_uuid'] = current_reference_uuid
    session['user_id'] = user_id
    session['app_id'] = app_id

def cache_data_privacy(wfk):
    try:
        logger = Logger()
        flow_tracking = LoginData().get_flow_tracking()
        login_info = flow_tracking.get('login_info', {})
        app_id = login_info.get('app_id')
        _11638 = login_info.get('data_processor')
        _11639 = login_info.get('data_controller')
        data_processor_list = []
        data_controller_list = []
        logger.debug('cache_data_privacy.wfk: {}'.format(wfk))
        logger.debug('cache_data_privacy.app_id: {}'.format(app_id))
        logger.debug('cache_data_privacy._11638: {}'.format(_11638))
        logger.debug('cache_data_privacy._11639: {}'.format(_11639))
        if app_id and _11638 and _11639:
            param = {
                'request_data': {
                    'query': ' select * from 1_2108 where _10011 = {} and _11638 = "{}" and _11639 = "{}" '.format(app_id, _11638, _11639),
                    '$options': {
                        'embed_public': False
                    }
                }
            }
            logger.debug('cache_data_privacy.param dp : {}'.format(param))
            result = Launcher(logger).service_manager('data_query_journal_with_tql', param)
            logger.debug('cache_data_privacy.result dp : {}'.format(result))
            if result.ok :
                result_json = result.json()
                logger.debug('cache_data_privacy.result 1 : {}'.format(result_json))
                if(result_json['msg_code'] == '30000'):
                    for item in result_json['response_data']:
                        data_processor_list.append(item['_1'])
            param = {
                'request_data': {
                    'query': ' select * from 1_2387 where _10011 = {} '.format(app_id),
                    '$options': {
                        'embed_public': False
                    }
                }
            }
            logger.debug('cache_data_privacy.param dc : {}'.format(param))
            result = Launcher(logger).service_manager('data_query_journal_with_tql', param)
            logger.debug('cache_data_privacy.result dc : {}'.format(result))
            if result.ok :
                result_json = result.json()
                logger.debug('cache_data_privacy.result 2 : {}'.format(result_json))
                if(result_json['msg_code'] == '30000'):
                    for item in result_json['response_data']:
                        data_controller_list.append(item['_1'])
        user_id = login_info.get('user_id')
        key = '{}_{}_data_privacy'.format(app_id, user_id)
        data = {
            'data_processor': data_processor_list,
            'data_controller': data_controller_list
        }
        logger.debug('cache_data_privacy.data: {}'.format(data))
        Cache().put_json(key, data)
    except Exception as error:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        exception_message = exc_type, fname, exc_tb.tb_lineno
        logger.error('cache_data_privacy.error: {}'.format(error))
        logger.error('cache_data_privacy.exception_message: {}'.format(exception_message))

blah_bp = Blueprint('blah_bp', __name__, template_folder = 'app')

@app.before_request
def before_request():
    jaeger_util = JaegerUtil()
    jaeger_util.start('mbs-node-v7')
    # jaeger_util.booking(request.path, None, None, None)

# @app.after_request
# def after_request_func(response):
#     jaeger_util = JaegerUtil()
#     jaeger_util.start('mbs-node-v7')
#     jaeger_util.booking('{}_out'.format(request.path), None, None)
#     return response

@blah_bp.route('/home', strict_slashes=False, methods=['GET'])
def indexInitial():
    response = None
    if JaegerUtil.tracer:
        with JaegerUtil.tracer.start_active_span(request.path) as scope:
            if scope and scope.span:
                scope.span.log_kv(None)
                JaegerUtil.active_span = scope.span
                start_time = time.time()
                generate_session()
                logger = Logger()
                logger.info('start call route - url: /{}'.format('home'))
                wfk = session.get('wfk')
                cache_data_privacy(wfk)
                response = send_from_directory('js', 'mbs-{}.js'.format(constants.INSTANCE_UUID))
                end_time = time.time()
                used_time = (end_time - start_time) * 1000
                logger.info('end call route - url: /{} - used_time: {}'.format('home', used_time))
    return response

@blah_bp.route('/get_configuration', strict_slashes=False, methods=['GET'])
@no_cache
def configurationInitial():
    response = None
    if JaegerUtil.tracer:
        with JaegerUtil.tracer.start_active_span(request.path) as scope:
            if scope and scope.span:
                scope.span.log_kv(None)
                JaegerUtil.active_span = scope.span
                start_time = time.time()
                generate_session()
                logger = Logger()
                logger.info('start call route - url: /{}'.format('get_configuration'))
                response = ConfigurationViews().call()
                LoginData().clear_flow_tracking_cache()
                end_time = time.time()
                used_time = (end_time - start_time) * 1000
                logger.info('end call route - url: /{} - used_time: {}'.format('get_configuration', used_time))
    return response

@blah_bp.route('/service_manager', strict_slashes=False, methods=['POST', 'GET'])
@no_cache
def serviceManagerPostInitial():
    response = None
    if JaegerUtil.tracer:
        with JaegerUtil.tracer.start_active_span(request.path) as scope:
            if scope and scope.span:
                scope.span.log_kv(None)
                JaegerUtil.active_span = scope.span
                start_time = time.time()
                generate_session()
                request_wrapping = RequestWrapping()
                request_wrapping.decryption(request.json)
                logger = Logger()
                logger.info('start call route - url: /{}'.format('service_manager'))
                response = ServiceManagerViews().call()
                LoginData().clear_flow_tracking_cache()
                end_time = time.time()
                used_time = (end_time - start_time) * 1000
                logger.info('end call route - url: /{} - used_time: {}'.format('service_manager', used_time))
    return response

@blah_bp.route('/service_manager_direct', strict_slashes=False, methods=['POST', 'GET'])
@no_cache
def serviceManagerDirectPostInitial():
    response = None
    if JaegerUtil.tracer:
        with JaegerUtil.tracer.start_active_span(request.path) as scope:
            if scope and scope.span:
                scope.span.log_kv(None)
                JaegerUtil.active_span = scope.span
                start_time = time.time()
                generate_session()
                logger = Logger()
                logger.info('start call route - url: /{}'.format('service_manager_direct'))
                response = ServiceManagerDirectViews().call()
                LoginData().clear_flow_tracking_cache()
                end_time = time.time()
                used_time = (end_time - start_time) * 1000
                logger.info('end call route - url: /{} - used_time: {}'.format('service_manager_direct', used_time))
    return response

@blah_bp.route('/service_manager_multiple', strict_slashes=False, methods=['POST', 'GET'])
@no_cache
def serviceManagerMultiplePostInitial():
    response = None
    if JaegerUtil.tracer:
        with JaegerUtil.tracer.start_active_span(request.path) as scope:
            if scope and scope.span:
                scope.span.log_kv(None)
                JaegerUtil.active_span = scope.span
                start_time = time.time()
                generate_session()
                logger = Logger()
                logger.info('start call route - url: /{}'.format('service_manager_multiple'))
                response = ServiceManagerMultipleViews().post()
                LoginData().clear_flow_tracking_cache()
                end_time = time.time()
                used_time = (end_time - start_time) * 1000
                logger.info('end call route - url: /{} - used_time: {}'.format('service_manager_multiple', used_time))
    return response

@blah_bp.route('/get_intent', strict_slashes=False, methods=['POST', 'GET'])
@no_cache
def get_intent():
    response = None
    if JaegerUtil.tracer:
        with JaegerUtil.tracer.start_active_span(request.path) as scope:
            if scope and scope.span:
                scope.span.log_kv(None)
                JaegerUtil.active_span = scope.span
                start_time = time.time()
                generate_session()
                logger = Logger()
                logger.info('start call route - url: /{}'.format('get_intent'))
                response = None
                try:
                    response = GetcacheViews().post()
                except Exception as e:
                    response = {
                        'internal_server_error': str(e)
                    }
                LoginData().clear_flow_tracking_cache()
                end_time = time.time()
                used_time = (end_time - start_time) * 1000
                logger.info('end call route - url: /{} - used_time: {}'.format('get_intent', used_time))
    return jsonify(response)

@blah_bp.route('/put_intent', strict_slashes=False, methods=['POST', 'GET'])
@no_cache
def put_intent():
    response = None
    if JaegerUtil.tracer:
        with JaegerUtil.tracer.start_active_span(request.path) as scope:
            if scope and scope.span:
                scope.span.log_kv(None)
                JaegerUtil.active_span = scope.span
                start_time = time.time()
                generate_session()
                logger = Logger()
                logger.info('start call route - url: /{}'.format('put_intent'))
                response = PutcacheViews().call()
                LoginData().clear_flow_tracking_cache()
                end_time = time.time()
                used_time = (end_time - start_time) * 1000
                logger.info('end call route - url: /{} - used_time: {}'.format('put_intent', used_time))
    return response

@blah_bp.route('/search', strict_slashes=False, methods=['POST'])
@no_cache
def searchInitial():
    response = None
    if JaegerUtil.tracer:
        with JaegerUtil.tracer.start_active_span(request.path) as scope:
            if scope and scope.span:
                scope.span.log_kv(None)
                JaegerUtil.active_span = scope.span
                start_time = time.time()
                generate_session()
                logger = Logger()
                logger.info('start call route - url: /{}'.format('search'))
                response = SearchViews().call()
                LoginData().clear_flow_tracking_cache()
                end_time = time.time()
                used_time = (end_time - start_time) * 1000
                logger.info('end call route - url: /{} - used_time: {}'.format('search', used_time))
    return response

@blah_bp.route('/logging', strict_slashes=False, methods=['POST'])
@no_cache
def logging():
    response = None
    if JaegerUtil.tracer:
        with JaegerUtil.tracer.start_active_span(request.path) as scope:
            if scope and scope.span:
                scope.span.log_kv(None)
                JaegerUtil.active_span = scope.span
                start_time = time.time()
                generate_session()
                logger = Logger()
                logger.info('start call route - url: /{}'.format('logging'))
                data = request.json
                logger.error('MBS frontend : {}'.format(data['errorMessage']))
                end_time = time.time()
                used_time = (end_time - start_time) * 1000
                logger.info('end call route - url: /{} - used_time: {}'.format('logging', used_time))
    return jsonify({})

@blah_bp.route('/monitor', strict_slashes=False, methods=['POST'])
@no_cache
def monitor():
    response = None
    if JaegerUtil.tracer:
        with JaegerUtil.tracer.start_active_span(request.path) as scope:
            if scope and scope.span:
                scope.span.log_kv(None)
                JaegerUtil.active_span = scope.span
                start_time = time.time()
                generate_session()
                logger = Logger()
                logger.info('start call route - url: {}'.format(request.path))
                data = request.json
                logger.info('monitor request ({}) - frontend: {}'.format(request.path, data))
                end_time = time.time()
                used_time = (end_time - start_time) * 1000
                logger.info('end call route - url: {} - used_time: {}'.format(request.path, used_time))
    return jsonify({})