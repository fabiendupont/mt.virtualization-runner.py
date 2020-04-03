import guestfs
import logging
import nbd
import os
import subprocess
import tempfile
import time

from pyVim.task import WaitForTask

class GuestfsHelper:
    def __init__(self, request, side, state):
        self._request = request
        self._side = side
        self._state = state


    def _create_password_file(self, password, pwddir="/tmp"):
        fd, path = tempfile.mkstemp(suffix='.v2v', dir=pwddir)
        try:
            os.write(fd, bytes(password.encode('utf-8')))
        finally:
            os.close(fd)
        return path


    def _get_thumbprint(self, host, port=443):
        import ssl
        import socket
        import hashlib

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(1)
        wrapped_socket = ssl.wrap_socket(sock)

        try:
            wrapped_socket.connect((host, port))
        except:
            raise Exception("Couldn't connect to %s:%s to retrieve thumbprint" % (host, port))
        else:
            tp = hashlib.sha1(wrapped_socket.getpeercert(True)).hexdigest().upper()
            return ":".join([tp[i:i+2] for i in range(0, len(tp), 2)])


    def _get_nbdkit_cmd_ssh(self, disk_spec, socket_path, **kwargs):
        nbdkit_cmd = [
            "sudo",
            "LIBGUESTFS_DEBUG=1", "LIBGUESTFS_TRACE=1",
            "nbdkit",
        ]

        if not socket_path is None:
            nbdkit_cmd.extend(["--unix", socket_path])
        else:
            nbdkit_cmd.append("--single")

        nbdkit_cmd.extend([
            "--exit-with-parent",
            "--read-only",
            "--verbose",
            "--exportname=/",
            "--filter=log",
            # "--filter=cacheextents", # Requires nbdkit >= 1.15
            "ssh",
            "verify-remote-host=false",
            "host=%s" % kwargs["hostname"],
            "user=%s" % kwargs["username"],
            "password=+%s" % self._create_password_file(kwargs["password"], pwddir="/tmp/%s" % self._request["vm_name"]),
            "path=%s" % disk_spec["absolute_path"],
            "logfile=/tmp/%s/nbd/%s-%s" % (self._request["vm_name"], disk_spec["id"], kwargs["sync_index"])
        ])
        return nbdkit_cmd


    def _get_nbdkit_cmd_vddk(self, disk_spec, socket_path, **kwargs):
        nbdkit_env = os.environ.copy()
        nbdkit_env = "LD_LIBRARY_PATH=%s/lib64" % kwargs["vddk_path"]
        if "LD_LIBRARY_PATH" in os.environ:
            nbdkit_env += ":%s" % os.environ["LD_LIBRARY_PATH"]

        nbdkit_cmd = [
            "sudo",
            "env", nbdkit_env,
            "LIBGUESTFS_DEBUG=1", "LIBGUESTFS_TRACE=1",
            "nbdkit"
        ]

        if socket_path is not None:
            nbdkit_cmd.extend(["--unix", socket_path])
        else:
            nbdkit_cmd.append("--single")

        nbdkit_cmd.extend([
            "--exit-with-parent",
            "--read-only",
            "--verbose",
            "--exportname=/",
            # "--filter=cacheextents", # Requires nbdkit >= 1.15
            "vddk",
            "libdir=%s" % kwargs["vddk_path"],
            "server=%s" % kwargs["hostname"],
            "thumbprint=%s" % self._get_thumbprint(kwargs["hostname"]),
            "user=%s" % kwargs["username"],
            "password=+%s" % self._create_password_file(kwargs["password"], pwddir="/tmp/%s" % self._request["vm_name"]),
            "file=%s" % disk_spec["path"],
            "vm=moref=%s" % kwargs["vm_moref"]
        ])

        if "snapshot_moref" in kwargs:
            nbdkit_cmd.extend(["snapshot=moref=%s" % kwargs["snapshot_moref"]])

        return nbdkit_cmd


    # TODO: Add support for rate filter
    def nbd_expose_disk_cmd(self, disk_spec, socket_path, **kwargs):
        if "vm_moref" not in kwargs:
            raise Exception("Neither 'vm_moref', nor 'snapshot_moref' key was supplied. Can't create NBD server.")
        vm_moref = kwargs["vm_moref"]

        hostname = self._request["source"]["authentication"]["host"]["hostname"]
        username = self._request["source"]["authentication"]["host"]["username"]
        password = self._request["source"]["authentication"]["host"]["password"]


        nbdkit_cmd = getattr(self, "_get_nbdkit_cmd_%s" % self._request["source"]["transport_method"])(
            disk_spec,
            socket_path,
            hostname=hostname,
            username=username,
            password=password,
            vm_moref=vm_moref,
            sync_index=(
                kwargs["sync_index"]
            ) if "sync_index" in kwargs else 0,
            snapshot_moref=(
                kwargs["snapshot_moref"]
            ) if "snapshot_moref" in kwargs else None,
            vddk_path=(
                kwargs["vddk_path"]
            ) if "vddk_path" in kwargs else "/opt/vmware-vix-disklib-distrib"
        )

        return nbdkit_cmd


    def nbd_process_aio_requests(self, nbd_handle):
        while nbd_handle.poll(0) == 1:
            pass


    def nbd_wait_for_aio_commands_to_finish(self, nbd_handle):
        while nbd_handle.aio_in_flight() > 0:
            nbd_handle.poll(-1)


    def get_vm_operating_system(self, vm_hardware, snapshot_moref=None, extended=False):
        if snapshot_moref is not None:
            return self._get_vm_operating_system_disk(vm_hardware, snapshot_moref, extended)
        elif vm_username is not None and vm_password is not None:
            return self._get_vm_operating_system_guest_tools(vm_hardware, snapshot_moref, extended)
        else:
            return {}


    def _get_vm_operating_system_guest_tools(self, vm_hardware, snapshot_moref=None, extended=False):
        return {}


    def _get_vm_operating_system_disk(self, vm_hardware, snapshot_moref=None, extended=False):
        sockets_paths = []
        nbd_servers = []
        for disk in vm_hardware["disks"]:
            socket_path = "/tmp/%s/%s.sock" % (self._request["vm_name"], self._request["vm_uuid"])
            nbdkit_cmd = self.nbd_expose_disk_cmd(disk, socket_path, vm_moref=vm_hardware["metadata"]["vm_moref"], snapshot_moref=snapshot_moref, sync_index=-1)
            logging.debug("NBDKIT COMMAND: %s", nbdkit_cmd)
            nbd_server = subprocess.Popen(nbdkit_cmd)

            # Allowing some time for the socket to be created
            for i in range(10):
                if os.path.exists(socket_path):
                    break
                time.sleep(1)

            sockets_paths.append(socket_path)
            nbd_servers.append(nbd_server)

        try:
            g = guestfs.GuestFS(python_return_dict=True)
            g.set_backend("direct")
            for socket_path in sockets_paths:
                g.add_drive_opts("", protocol="nbd", format="raw", server=["unix:%s" % socket_path], readonly=1)
            g.launch()

            roots = g.inspect_os()
            if len(roots) == 0:
                raise(Exception("inspect_os: no operating systems found"))

            operating_systems = []
            for root in roots:
                operating_system = {
                    "arch": g.inspect_get_arch(root),
                    "type": g.inspect_get_type(root),
                    "distro": g.inspect_get_distro(root),
                    "product_name": g.inspect_get_product_name(root),
                    "product_variant": g.inspect_get_product_variant(root),
                    "package_format": g.inspect_get_package_format(root),
                    "package_management": g.inspect_get_package_management(root),
                    "mountpoints": g.inspect_get_mountpoints(root),
                    "major_version": g.inspect_get_major_version(root),
                    "minor_version": g.inspect_get_minor_version(root),
                    "hostname": g.inspect_get_hostname(root),
                    "filesystems": g.inspect_get_filesystems(root)
                }

                if operating_system["type"] == "windows":
                    operating_system["drive_mappings"] = g.inspect_get_drive_mappings(root)
                    operating_system["current_control_set"] = g.inspect_get_windows_current_control_set(root)
                    operating_system["windows_software_hive"] = g.inspect_get_windows_software_hive(root)
                    operating_system["windows_system_hive"] = g.inspect_get_windows_system_hive(root)
                    operating_system["windows_systemroot"] = g.inspect_get_windows_systemroot(root)

                operating_systems.append(operating_system)

        except Exception as error:
            raise error
        finally:
            for nbd_server in nbd_servers:
                nbd_server.kill()
            for socket_path in sockets_paths:
                os.remove(socket_path)

        return operating_systems[0]
