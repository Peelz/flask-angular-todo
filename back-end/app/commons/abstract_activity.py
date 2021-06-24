import sys
import os
import json
import re
import uuid

from datetime import datetime

from flask import request, jsonify
from flask import current_app as app
from flask_api import status

from app.commons import constants
from app.commons.services.log import Logger
from app.commons.services.cache import Cache
from app.commons.services.common_action import CommonAction
from app.commons.services.ui_response import UIResponse
from app.commons.connectors.launcher import Launcher
from app.commons.abstract_flow_tracking_information import AbstractFlowTrackingInformation

class MethodNotAllowError(Exception):
    pass


class AbstractActivity(AbstractFlowTrackingInformation):

    def __init__(self):
        try:
            super().__init__()
            self.ui_response = UIResponse(self.logger)
            self.cache = Cache()
            self.common_action = CommonAction(self.logger)
            self.launcher_service = Launcher(self.logger)
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            exception_message = exc_type, fname, exc_tb.tb_lineno
            self.logger.error('AbstractActivity.__init__.e: {}'.format(e))
            self.logger.error('AbstractActivity.__init__.exception_message: {}'.format(exception_message))


    def call(self):
        try:
            if not app.config.get('LOCAL') or False:
                if 'HTTP_ORIGIN' not in request.environ or not re.search(r'{}'.format(constants.CORS_ALLOW_ORIGIN), request.environ['HTTP_ORIGIN']):
                    return '', 403

            try:
                func = getattr(self, request.method.lower())
            except:
                raise MethodNotAllowError   

            return func()
        except MethodNotAllowError:
            response = self.ui_response.error('method not allowed', self.reference_id)
            return jsonify(response), status.HTTP_200_OK
        except Exception as exc:
            response = self.ui_response.error(str(exc), self.reference_id)
            return jsonify(response), status.HTTP_200_OK
