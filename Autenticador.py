import os
import json


def get_user_pass(tool):
    if tool == 'sql':
        token = 'ABCDEFGH123'
    else:
        token = 'ZXCVBNM123'
    try:
        script = 'cm-app-cli'
        path_script = '/opt/app-imob/'
        cmd = 'python3' + path_script + script + '--path /opt/app_imob --token %s' % token

        cmd_result = os.popen(cmd).read()

        cm_data = json.loads(cmd_result)

    except Exception as e:

        print(f'Exception during read User and Password: {str(e)}')

        return {'user': None, 'password': None}

    print('token :' + token, '\nuser: ' + cm_data["user"])
    return cm_data["user"], cm_data["password"]
