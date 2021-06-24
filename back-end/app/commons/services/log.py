import uuid

from flask import current_app as app
from tisco.core.tfg_log.logger import TfgLogger

from app.commons import constants
from flask import request, session
from app.commons.services.cache import Cache

class Logger:

    def __init__(self):
        self.tfg_log = TfgLogger.getLogger(
            REF_ID=session.get('reference_id'),
            WFK=session.get('wfk'),
            MBS_REF_UUID=session.get('current_reference_uuid'),
            FLOW_CODE=session.get('code'),
            MBS_CODE=constants.INSTANCE_CODE,
            MBS_UUID=constants.INSTANCE_UUID,
            MBS_NAME=constants.INSTANCE_NAME,
            APP_ID=session.get('app_id'),
            USER_ID=session.get('user_id'),
            STATE=app.config.get('CURRENT_STATE'),
            SUB_STATE=app.config.get('CURRENT_SUB_STATE')
        )

    def info(self, message):
        self.tfg_log.info(message)

    def debug(self, message):
        self.tfg_log.debug(message)

    def error(self, message):
        self.tfg_log.error(message)

