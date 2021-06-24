
from flask import request

class RequestLogging:

    def log_request(self, logger, url, data):
        try:
            if data:
                logger.info('request logging ({}) - log_request: {}'.format(url, data))
            elif request.json:
                logger.info('request logging ({}) - log_request: {}'.format(url, request.json))
            else:
                logger.info('request logging ({}) - log_request: {}'.format(url, '-'))
        except Exception as e:
            logger.error('request logging error ({}) - log_request: {}'.format(url, e))
    
    def log_response(self, logger, url, data):
        try:
            logger.info('request logging ({}) - log_response ({}): {}'.format(url, type(data), data))
        except Exception as e:
            logger.error('request logging error ({}) - log_response: {}'.format(url, e))