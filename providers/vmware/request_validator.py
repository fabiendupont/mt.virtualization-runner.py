class RequestSourceValidator(object):

    REQUIRED_AUTHENTICATIONS = ["manager", "host"]
    SUPPORTED_TRANSPORT_METHODS = ['vddk', 'ssh']

    def __init__(self, request):
        self._request = request
        self._errors = []


    def validate(self):
        for auth in self.REQUIRED_AUTHENTICATIONS:
            if auth not in self._request["source"]["authentication"]:
                self._errors.append("Missing '%s' in request['source']['authentication']" % auth)
            if "hostname" not in self._request["source"]["authentication"][auth]:
                self._errors.append("Missing 'hostname' in request['source']['authentication']['%s']" % auth)

        if self._request["source"]["transport_method"] not in self.SUPPORTED_TRANSPORT_METHODS:
            self._errors.append("Transport method '%s' is not supported" % self._request["source"]["transport_method"])

        getattr(self, '_validate_authentication_' + self._request["source"]["transport_method"])

        return self._errors


    def _validate_authentication_vddk(self):
        if "username" not in self._request["source"]["authentication"]:
            self._errors.append("Missing 'username' in request['source']['authentication']")
        if "password" not in self._request["source"]["authentication"]:
            self._errors.append("Missing 'password' in request['source']['authentication']")


    def _validate_authentication_ssh(self):
        if "username" not in self._request["source"]["authentication"]:
            self._errors.append("Missing 'username' in request['source']['authentication']")
        if "ssh_key" not in self._request["source"]["authentication"]:
            self._errors.append("Missing 'ssh_key' in request['source']['authentication']")
