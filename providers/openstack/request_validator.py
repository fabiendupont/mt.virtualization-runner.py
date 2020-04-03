class Virtv2vBuilderDestinationOpenstack(Virtv2vBuilderDestination):

    REQUIRED_AUTHENTICATIONS = ["manager"]
    SUPPORTED_TRANSPORT_METHODS = ['cinder']

    def __init__(self, request, uuid):
        self._request = request
        self._errors = []


    def validate(self):
        for auth in self.REQUIRED_AUTHENTICATIONS:
            if auth not in self._request["source"]["authentication"]:
                self._errors.append("Missing '%s' in request['source']['authentication']" % auth)
            if "hostname" not in self._request["source"]["authentication"][auth]:
                self._errors.append("Missing 'hostname' in request['source']['authentication']['%s']" % auth)

        if self._destination_transport_method not in self.SUPPORTED_TRANSPORT_METHODS:
            self._errors.append("Transport method '%s' is not supported with %s as destination" % self._destination_transport_method)

        getattr(self, '_validate_authentication_' + self._destination_transport_method)

        if 'project' not in self._request['destination']:
            self._errors.append("Missing 'project' in request['destination']")

        if 'volume_type' not in self._request['destination']:
            self._errors.append("Missing 'volume_type' in request['destination']")

        if 'flavor' not in self._request['destination']:
            self._errors.append("Missing 'flavor' in request['destination']")

        if 'security_groups' not in self._request['destination']:
            self._errors.append("Missing 'security_groups' in request['destination']")

        if 'network_ports' not in self._request['destination']:
            self._errors.append("Missing 'network_ports' in request['destination']")

        return self._errors


    def _validate_authentication_cinder(self):
        if 'domain' not in self._request['destination']['authentication']:
            self._errors.append(raise "Missing 'domain' in request['destination']['authentication']")
        if 'username' not in self._request['destination']['authentication']:
            self._errors.append("Missing 'username' in request['destination']['authentication']")
        if 'password' not in self._request['destination']['authentication']:
            self._errors.append("Missing 'password' in request['destination']['authentication']")
        if 'project_name' not in self._request['destination']['authentication']:
            self._errors.append("Missing 'project_name' in request['destination']['authentication']")
