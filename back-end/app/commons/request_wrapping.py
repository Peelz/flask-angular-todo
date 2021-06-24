import os
import sys

from flask import current_app as app

from app.commons.services.log import Logger
from app.commons import constants_variable

from Crypto.Cipher import PKCS1_v1_5
from Crypto.PublicKey import RSA
from base64 import b64decode, b16decode

from flask import request

from app.commons import constants

class RequestWrapping:

    def decryption(self, data):
        try:
            logger = Logger()
            logger.info('start call request wrapping (v0.3)')
            param = data
            logger.info('RequestWrapping.decryption.in.param: {}'.format(param))
            if not request.form:
                enc_req = False
                if app.config.get('ENC_REQ') == 'Y':
                    enc_req = True
                enc_k = []
                if app.config.get('ENC_K'):
                    keys = app.config.get('ENC_K').split(',')
                    clean_keys = []
                    for x in keys:
                        clean_keys.append(x.strip())
                    enc_k = clean_keys
                if enc_req:
                    targets = []
                    if enc_k and len(enc_k) > 0:
                        targets = enc_k
                    else:
                        targets = constants_variable.E_R_K
                    for key in targets:
                        key_name = key
                        try:
                            value = None
                            keyPy = ''
                            for x in key.split('.'):
                                keyPy = keyPy + '["{}"]'.format(x)
                            isExist = False
                            try:
                                comand = 'value = param{}'.format(keyPy)
                                variable = locals()
                                exec(comand, globals(), variable)
                                value = variable['value']
                                isExist = True
                            except Exception as error:
                                isExist = False
                            if isExist:
                                decrypted = ''
                                if not isinstance(value, list):
                                    value = [value]
                                for x in value:
                                    et = x[-2:]
                                    encrypted = x[:-2]
                                    if et == 'v1':
                                        et = '1024'
                                    elif et == 'v2':
                                        et = '2048'
                                    elif et == 'v3':
                                        et = '4096'
                                    else:
                                        raise Exception('encrypt type not found')
                                    f = open('app/commons/key/dec-{}.pem'.format(et), 'rb')
                                    key = RSA.import_key(f.read())
                                    decrypt = PKCS1_v1_5.new(key)
                                    try:
                                        encrypted = b64decode(encrypted)
                                        if len(encrypted) == 127:
                                            hex_fixed = '00{}'.format(encrypted.hex())
                                            encrypted = b16decode(hex_fixed.upper())
                                        decode = decrypt.decrypt(encrypted, None)
                                        if isinstance(decode, bytes):
                                            decode = decode.decode("utf-8")
                                        decrypted = decrypted + decode
                                    except Exception as error:
                                        exc_type, exc_obj, exc_tb = sys.exc_info()
                                        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                                        exception_message = exc_type, fname, exc_tb.tb_lineno
                                        logger.error('RequestWrapping.decryption.decrypt.error: {}'.format(error))
                                        logger.error('RequestWrapping.decryption.decrypt.exception_message: {}'.format(exception_message))
                                        raise Exception(error)
                                try:
                                    mbs_code = None
                                    if key_name == 'service_name':
                                        decrypted_splits = decrypted.split('||')
                                        decrypted = decrypted_splits[0]
                                        mbs_code = decrypted_splits[-1]
                                        if not mbs_code or mbs_code != getattr(constants, 'INSTANCE_CODE', None):
                                            raise Exception('401 Unauthorized - Invalid mbs(mbs_code)')
                                    if decrypted == '<__NULL__>':
                                        decrypted = None
                                    comand = 'param{} = decrypted'.format(keyPy)
                                    exec(comand)
                                except Exception as error:
                                    exc_type, exc_obj, exc_tb = sys.exc_info()
                                    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                                    exception_message = exc_type, fname, exc_tb.tb_lineno
                                    logger.error('RequestWrapping.decryption.exec.error: {}'.format(error))
                                    logger.error('RequestWrapping.decryption.exec.exception_message: {}'.format(exception_message))
                                    raise Exception(error)
                        except Exception as error:
                            exc_type, exc_obj, exc_tb = sys.exc_info()
                            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                            exception_message = exc_type, fname, exc_tb.tb_lineno
                            logger.error('RequestWrapping.decryption.targets.error: {}'.format(error))
                            logger.error('RequestWrapping.decryption.targets.exception_message: {}'.format(exception_message))
                            raise Exception(error)
                    param.update(param)
                    if '_mbs_et_v_' in param:
                        param.pop('_mbs_et_v_')
            logger.info('request wrapping - param: {}'.format(param))
        except Exception as error:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            exception_message = exc_type, fname, exc_tb.tb_lineno
            logger.error('RequestWrapping.decryption.error: {}'.format(error))
            logger.error('RequestWrapping.decryption.exception_message: {}'.format(exception_message))
            raise Exception('request not secure')