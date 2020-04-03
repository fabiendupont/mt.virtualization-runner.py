from importlib import import_module

class RequestValidator:

    def __init__(self, request):
        self._request = request
        self._errors = []

    def validate(self):
        if 'conversion_host_uuid' not in self._request:
            self._errors.append("Missing 'conversion_host_uuid' in request")

        if "vm_name" not in self._request:
            self._errors.append("Missing 'vm_name' key in request")

        if "vm_uuid" not in self._request:
            self._errors.append("Missing 'vm_uuid' key in request")

        self._errors.extend(RequestSourceValidator(self._request).validate())
        self._errors.extend(RequestDestinationValidator(self._request).validate())

        return list(dict.fromkeys(self._errors))


class RequestSourceValidator:

    SUPPORTED_SOURCE_TYPES = ["vmware"]

    def __init__(self, request):
        self._request = request
        self._errors = []


    def validate(self):
        if "source" not in self._request:
            self._errors.append("Missing 'source' key in request")

        if "type" not in self._request["source"]:
            self._errors.append("Missing 'type' key in request['source']")
        if self._request["source"]["type"] not in self.SUPPORTED_SOURCE_TYPES:
            self._errors.append("Source type '%s' is not supported" % self._request["source"]["type"])

        if "transport_method" not in self._request["source"]:
            self._errors.append("Missing 'transport_method' key in request['source']")

        if "authentication" not in self._request["source"]:
            self._errors.append("Missing 'authentication' in request['source']")

        request_source_validator = import_module("providers.%s.request_validator" % self._request["source"]["type"])
        self._errors.extend(request_source_validator.RequestSourceValidator(self._request).validate())

        return self._errors


class RequestDestinationValidator:

    SUPPORTED_DESTINATION_TYPES = ["ovirt", "openstack"]

    def __init__(self, request):
        self._request = request
        self._errors = []


    def validate(self):
        if "destination" not in self._request:
            self._errors.append("Missing 'destination' in request")

        if "type" not in self._request["destination"]:
            self._errors.append("Missing 'type' key in request['destination']")
        if self._request["destination"]["type"] not in self.SUPPORTED_DESTINATION_TYPES:
            self._errors.append("Destination type '%s' is not supported" % self._request["destination"]["type"])

        if "transport_method" not in self._request["destination"]:
            self._errors.append("Missing 'transport_method' key in request['destination']")

        if "authentication" not in self._request["destination"]:
            self._errors.append("Missing 'authentication' in request['destination']")

        request_destination_validator = import_module("providers.%s.request_validator" % self._request["destination"]["type"])
        self._errors.extend(request_destination_validator.RequestDestinationValidator(self._request).validate())

        return self._errors
