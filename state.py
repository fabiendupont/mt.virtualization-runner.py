import json
import logging
import tempfile
import threading
import os

class State:
    """
    State object (which is a dict inside)
    """

    def __init__(self, path):
        self._path = path
        self.finished = False
        self.failed = False
        self.disks = {}
        self.throttling = {
            "cpu": None,
            "network": None
        }
        self.write()


    def __str__(self):
        state = {
            "finished": self.finished,
            "failed": self.failed,
            "disks": self.disks,
            "throttling": self.throttling
        }
        return json.dumps(state)


    def write(self):
        tmp_fd, tmp_path = tempfile.mkstemp(suffix = '.v2v.state')
        with os.fdopen(tmp_fd, 'w') as fd:
            fd.write(str(self))
        os.rename(tmp_path, self._path)
