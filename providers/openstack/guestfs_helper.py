import os
import logging
import subprocess
from importlib import import_module

class GuestfsHelper:
    def __init__(self, request, side, state):
        self._request = request
        self._side = side
        self._state = state


    def nbd_expose_disk_cmd(self, disk_spec, **kwargs):
        pass


    def nbd_process_aio_requests(self, nbd_handle):
        pass


    def nbd_wait_for_aio_commands_to_finish(self, nbd_handle):
        pass
