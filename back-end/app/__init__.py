import os
import logging.config

from flask import Flask, g
from app.commons import constants

from app.commons.middlewares.authentication import AuthenticationFilter
from tisco.core.tfg_log.logger import TfgLogger, log_decorator, LOGGING, ACTION_TYPE

def create_app(test_config=None):
    # create and configure the app
    app = Flask(__name__, instance_relative_config=True)

    if test_config is None:
        config_path = os.environ.get('APP_CONFIG_PATH', None)

        if not config_path:
            config_path = os.path.join(os.getcwd(), 'config')
        app.config.from_pyfile(os.path.join(config_path, 'app.cfg'), silent=True)
    else:
        app.config.from_mapping(test_config)

    logging.config.dictConfig(LOGGING)
    TfgLogger.setup(settings=app.config)

    # ensure the instance folder exists
    try:
        os.makedirs(app.instance_path)
    except OSError:
        pass

    # use Blueprint separate file (views.py)
    with app.app_context():
        from app.routing.views import blah_bp
        app.register_blueprint(blah_bp)

    if app.config.get('MBS_UUID'):
        setattr(constants, "INSTANCE_UUID", app.config.get('MBS_UUID'))

    if not app.config.get('LOCAL') or False:
        # middleware configuration
        app.wsgi_app = AuthenticationFilter(app)

    return app
    

