
class Tool:

    @staticmethod
    def remove_sensitive_key_for_logging(obj):
        output = {}
        sensitives = ['response_data', 'result', 'client_id', 'client_secret', 'data']
        for key, value in obj.items():
            if key not in sensitives:
                output[key] = value
            else:
                output[key] = '...'
        return output