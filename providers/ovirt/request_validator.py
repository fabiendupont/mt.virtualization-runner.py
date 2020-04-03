class RequestDestinationValidator(object):

    REQUIRED_AUTHENTICATIONS = ["manager"]
    SUPPORTED_TRANSPORT_METHODS = ['upload_api']

    def __init__(self, request):
        self._request = request
        self._errors = []

    def validate(self):
        for auth in self.REQUIRED_AUTHENTICATIONS:
            if auth not in self._request["source"]["authentication"]:
                self._errors.append("Missing '%s' in request['source']['authentication']" % auth)
            if "hostname" not in self._request["source"]["authentication"][auth]:
                self._errors.append("Missing 'hostname' in request['source']['authentication']['%s']" % auth)

        if self._request["destination"]["transport_method"] not in self.SUPPORTED_TRANSPORT_METHODS:
            self._errors.append("Transport method '%s' is not supported" % self._request["destination"]["transport_method"])

        getattr(self, '_validate_authentication_' + self._request["destination"]["transport_method"])

#        if "nics" not in self._request["destination"]:
#            self._errors.append("Missing 'nics' in request['destination']")

        if "cluster" not in self._request["destination"]:
            self._errors.append("Missing 'cluster' in request['destination']")

        if "storage" not in self._request["destination"]:
            self._errors.append("Missing 'storage' in request['destination']")

        return self._errors


    def _validate_authentication_upload_api(self):
        if "username" not in self._request["destination"]["authentication"]:
            self._errors.append("Missing 'username' in request['destination']['authentication']")
        if "password" not in self._request["destination"]["authentication"]:
            self._errors.append("Missing 'password' in request['destination']['authentication']")
