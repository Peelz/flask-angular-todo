from flask import request, session
from app.commons.services.cache import Cache
from flask import current_app as app
from app.commons.global_variable import GlobalVariable

class LoginData():

    def get_work_flow_key(self):
        workflowkey = session.get('wfk')
        if app.config.get('LOCAL') or False:
            workflowkey = '0bd34cac-3707-4e6d-a81b-8a9468642c32'
        return workflowkey
    
    def get_flow_tracking_with_wfk(self, workflowkey):
        flow_tracking_key = 'FLOWTRACKING_{}'.format(workflowkey)
        if flow_tracking_key in GlobalVariable.flowtracking:
            flow_tracking = GlobalVariable.flowtracking.get(flow_tracking_key, {})
        else:
            flow_tracking = Cache().get_json(flow_tracking_key)
            if flow_tracking:
                GlobalVariable.flowtracking[flow_tracking_key] = flow_tracking

        if app.config.get('LOCAL') or False:
            # mock
            flow_tracking = {
                'code': 'ShelfFlow_I9_D1_1',
                'current_reference_uuid': '04dec32d-5877-42cd-a129-fca465f50e02',
                'display_label': 'Identity',
                'instances': [
                    {
                        'auto_next': True,
                        'code': 'I9_D1_1',
                        'datafields': [],
                        'dataset': {
                            'description': 'Identity',
                            'name': '1_1401',
                            'version': '1.25',
                        },
                        'display_label': None,
                        'instance_type': 'common',
                        'intent': {
                            'ins': [],
                            'outs': [],
                        },
                        'name': 'เข้าแก้ไขข้อมูลแบบ key-in Identity',
                        'next_instances': [],
                        'reference_uuid': '04dec32d-5877-42cd-a129-fca465f50e02',
                        'sequence': 1,
                        'sub_sequence': 1,
                        'url': 'https://node-v7-state1-lb01.c1-alpha-tiscogroup.com/tg1/8349329d-c814-412a-a524-91d2bfd528c8/home',
                        'uuid': '8349329d-c814-412a-a524-91d2bfd528c8',
                        'version': '1',
                    }
                ],
                'login_info': {
                    'app_id': '65',
                    'app_label': None,
                    'branch': None,
                    'data_controller': None,
                    'data_processor': None,
                    'erm_role': 'c170a001-b31c-4e93-a00e-51e9e82ff630',
                    'firstname': 'Thanapol',
                    'lastname': 'Kurujitkosol',
                    'log_session_id': '44a32110-07ae-4f0b-af14-107b6d4e2d03',
                    'regis_service_id': '',
                    'sub_controller': '',
                    'sub_state': '',
                    'user_id': 'a7a05d0a-e4be-4a2c-a216-dc1e7be4a4ed',
                    'user_ucid': '',
                    'username': 'zzz@tisco.co.th',
                },
                'name': 'เข้าแก้ไขข้อมูลแบบ key-in Identity',
                'system_data': True,
                'uuid': 'd55ab41a-ba39-428d-b019-eaaf82fe4ab8',
                'workflow_key': '44a32110-07ae-4f0b-af14-107b6d4e2d03',
            }
        return flow_tracking

    def get_flow_tracking(self):
        workflowkey = self.get_work_flow_key()
        return self.get_flow_tracking_with_wfk(workflowkey)
        
    def clear_flow_tracking_cache(self):
        workflowkey = self.get_work_flow_key()
        flow_tracking_key = 'FLOWTRACKING_{}'.format(workflowkey)
        if flow_tracking_key in GlobalVariable.flowtracking:
            GlobalVariable.flowtracking.pop(flow_tracking_key)
