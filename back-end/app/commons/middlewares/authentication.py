import os
import sys
import redis
import json
import re
import urllib.parse as urlpr

from werkzeug.wrappers import Request, Response
from flask import Flask

from app.commons import constants


class AuthenticationFilter(object):

    def __init__(self, app):
        self.app = app
        self.wsgi_app = app.wsgi_app


    def __call__(self, environ, start_response):
        origin = environ.get('HTTP_ORIGIN', '')
        path_info = environ.get('PATH_INFO', '')
        http_method = environ.get('REQUEST_METHOD', '').upper()

        def custom_start_response(status, headers):
            if re.search(r'{}'.format(constants.CORS_ALLOW_ORIGIN), origin):
                headers = list(filter(lambda h: h[0] not in ['Access-Control-Allow-Origin', 'Access-Control-Allow-Methods'], headers))
                headers.append(('Access-Control-Allow-Origin', origin))
                headers.append(('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, HEAD, OPTIONS'))
                headers.append(('Access-Control-Allow-Headers', '*'))
            return start_response(status, headers)

        if '/HealthCheck' in path_info or http_method in ['HEAD', 'OPTIONS']:
            return self.wsgi_app(environ, custom_start_response)

        redisCache = redis.Redis(host=self.app.config.get('REDIS_URL'))
        if '/home' in path_info:
            query_strings = environ.get('QUERY_STRING', '').split('?')
            for query_string in query_strings:
                if query_string.startswith('wfk='):
                    wfk = query_string[4:]
                    break
        else:
            wfk = Request(environ, shallow=True).headers.get('wfk')
        
        if not wfk:
            response = Response('401 Unauthorized - Invalid flow session id', status=401)
            return response(environ, custom_start_response)

        flow_tracking = redisCache.get('FLOWTRACKING_{}'.format(wfk))
        if not flow_tracking:
            response = Response('401 Unauthorized - Flow session expired', status=401)
            return response(environ, custom_start_response)

        flow_tracking = json.loads(flow_tracking)
        current_node_sequence = flow_tracking.get('current_node_sequence')
        current_instance_sequence = flow_tracking.get('current_instance_sequence')
        nodes = flow_tracking.get('nodes')

        current_instance = dict()
        current_reference_uuid = flow_tracking.get('current_reference_uuid')
        instances = flow_tracking.get('instances') or list()
        for instance in instances:
            if current_reference_uuid == instance.get('reference_uuid'):
                current_instance = instance
        current_uuid = None
        if current_instance.get('instance_type') == 'common':
            current_uuid = current_instance.get('uuid', None)
        elif current_instance.get('instance_type') == 'combined':
            current_uuid = current_instance.get('base_instance_uuid', None)
        elif current_instance.get('instance_type') == 'custom':
            current_uuid = current_instance.get('base_instance_uuid', None)
        if current_instance.get('instance_type') != 'custom':
            if getattr(constants, 'INSTANCE_UUID', None) != current_uuid:
                response = Response(
                    json.dumps({"code": 'MBS_1001', "desc": "Invalid mbs(current_mbs)"}),
                    status=401,
                    mimetype="application/json",
                )
                return response(environ, custom_start_response)

        # support 'flow runtime' version section : begin #
        if nodes is None or current_node_sequence is None or current_instance_sequence is None:
            return self.wsgi_app(environ, custom_start_response)
        # support 'flow runtime' version section : end #

        if len(nodes) < current_node_sequence:
            response = Response('401 Unauthorized - Flow session expired (End flow)', status=401)
            return response(environ, custom_start_response)

        possible_instance_uuids = set()
        for node in nodes:
            for instance in node.get('instances') or list():
                base_uuid = instance.get('base_instance_uuid') or instance.get('uuid')
                # member_uuids = set(map(lambda member: member.get('uuid'), instance.get('member_instances') or list()))
            possible_instance_uuids.add(base_uuid)
            # possible_instance_uuids.update(member_uuids)

        if constants.INSTANCE_UUID not in possible_instance_uuids:
            response = Response('401 Unauthorized - Improper access', status=401)
            return response(environ, custom_start_response)
        
        return self.wsgi_app(environ, custom_start_response)
