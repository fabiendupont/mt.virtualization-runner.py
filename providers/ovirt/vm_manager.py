import logging
import ovirtsdk4
import ovirtsdk4.types
import subprocess
import time

class VmManager:

    def __init__(self, request, side, state):
        self._request = request
        self._side = side
        self._state = state
        self._connection = self._connect()


    def __del__(self):
        self._disconnect()


    def _connect(self):
        conn = ovirtsdk4.Connection(
            url = "https://%s/ovirt-engine/api" % self._request[self._side]["authentication"]["manager"]["hostname"],
            username = self._request[self._side]["authentication"]["manager"]["username"],
            password = self._request[self._side]["authentication"]["manager"]["password"],
            insecure = True
        )

        return conn


    def _disconnect(self):
        try:
            self._connection.close()
        except:
            pass


    def _find_vm_by_id(self, vm_id):
        return self._connection.system_service().vms_service().list(search = "id=%s" % vm_id)[0]


    def get_vm_hardware(self, object_type):
        pass


    def _generate_vm_ovf(self, vm_spec):
        from xml.etree import ElementTree as ET
        from xml.dom import minidom

        xml = ET.Element("domain", { "type": "kvm" })

        return ovf


    def _find_disk_by_id(self, disk_id):
        return self._connection.system_service().disks_service().list(search = "id=%s" % disk_id)[0]


    def _find_storage_domain_by_name(self, name):
        return self._connection.system_service().storage_domains_service().list(search="name=%s" % name)[0]


    def _best_fit_vm_type(self, os):
        if os["type"] == "linux" and os["major_version"] in [3, 4]:
            if "ES" in os["product_name"]:
                return ovirtsdk4.types.VmType("server")
            elif "AS" in os["product_name"]:
                return ovirtsdk4.types.VmType("server")
            else:
                return ovirtsdk4.types.VmType("desktop")
        else:
            if os["product_variant"] in ["Server", "Server Core", "Embedded"]:
                return ovirtsdk4.types.VmType("server")
            elif "Server" in os["product_name"]:
                return ovirtsdk4.types.VmType("server")
            elif "Desktop" in os["product_name"]:
                return ovirtsdk4.types.VmType("desktop")
            else:
                return ovirtsdk4.types.VmType("server")


    def _best_fit_os_type(self, os):
        if os["type"] == "linux" and os["distro"] in ["rhel", "centos"] and os["major_version"] <= 6 and os["arch"] == "i386":
            return ovirtsdk4.types.OperatingSystem("RHEL%d" % os["major_version"])
        elif os["type"] == "linux" and os["distro"] in ["rhel", "centos"] and os["major_version"] <= 6 and os["arch"] == "x86_64":
            return ovirtsdk4.types.OperatingSystem("RHEL%dx64" % os["major_version"])
        elif os["type"] == "linux" and os["distro"] in ["rhel", "centos"] and os["major_version"] <= 6 and os["arch"] in ["ppc64", "ppc64le"] and os["minor_version"] >= 9:
            return ovirtsdk4.types.OperatingSystem("rhel_6_9_plus_ppc64")
        elif os["type"] == "linux" and os["distro"] in ["rhel", "centos"] and os["major_version"] <= 6 and os["arch"] in ["ppc64", "ppc64le"]:
            return ovirtsdk4.types.OperatingSystem("rhel_6_ppc64")
        elif os["type"] == "linux" and os["distro"] in ["rhel", "centos"] and os["arch"] and os["arch"] == "x86_64":
            return ovirtsdk4.types.OperatingSystem("rhel_%dx64" % os["major_version"])
        elif os["type"] == "linux" and os["distro"] in ["rhel", "centos"] and os["arch"] and os["arch"] == "ppc64":
            return ovirtsdk4.types.OperatingSystem("rhel_7_ppc64")
        elif os["type"] == "linux" and os["distro"] in ["rhel", "centos"] and os["arch"] and os["arch"] == "s390x":
            return ovirtsdk4.types.OperatingSystem("rhel_7_s390x")
        elif os["type"] == "linux" and os["distro"] == "sles" and os["major_version"] == 11 and os["arch"] == "x86_64":
            return ovirtsdk4.types.OperatingSystem("sles_11")
        elif os["type"] == "linux" and os["distro"] == "sles" and os["major_version"] == 11 and os["arch"] in ["ppc64", "ppc64le"]:
            return ovirtsdk4.types.OperatingSystem("sles_11_ppc64")
        elif os["type"] == "linux" and os["distro"] == "sles" and os["major_version"] == 12 and os["arch"] == "s390x":
            return ovirtsdk4.types.OperatingSystem("sles_12_s390x")
        elif os["type"] == "linux" and os["distro"] == "debian" and os["major_version"] >= 7:
            return ovirtsdk4.types.OperatingSystem("debian_7")
        elif os["type"] == "linux" and os["distro"] == "ubuntu" and os["major_version"] >= 14 and os["arch"] == "ppc64":
            return ovirtsdk4.types.OperatingSystem("ubuntu_14_04_ppc64")
        elif os["type"] == "linux" and os["distro"] == "ubuntu" and os["major_version"] >= 16 and os["arch"] == "s390x":
            return ovirtsdk4.types.OperatingSystem("ubuntu_16_04_s390x")
        elif os["type"] == "linux" and os["distro"] == "ubuntu" and os["major_version"] >= 14:
            return ovirtsdk4.types.OperatingSystem("ubuntu_14_04")
        elif os["type"] == "linux" and os["distro"] == "ubuntu" and os["major_version"] >= 12:
            return ovirtsdk4.types.OperatingSystem("ubuntu_%d_%02d" % (os["marjor_version"], os["minor_version"]))
        elif os["type"] == "linux" and os["arch"] in ["ppc64", "ppc64le"]:
            return ovirtsdk4.types.OperatingSystem("other_linux_ppc64")
        elif os["type"] == "linux" and os["arch"] == "s390x":
            return ovirtsdk4.types.OperatingSystem("other_linux_s390x")
        elif os["type"] == "linux":
            return ovirtsdk4.types.OperatingSystem("OtherLinux")
        elif os["type"] == "windows" and os["major_version"] == 5 and os["minor_version"] == 1:
            return ovirtsdk4.types.OperatingSystem("WindowsXP")
        elif os["type"] == "windows" and os["major_version"] == 5 and os["minor_version"] == 2 and os["arch"] == "i386":
            return ovirtsdk4.types.OperatingSystem("Windows2003")
        elif os["type"] == "windows" and os["major_version"] == 5 and os["minor_version"] == 2 and os["arch"] == "x86_64":
            return ovirtsdk4.types.OperatingSystem("Windows2003x64")
        elif os["type"] == "windows" and os["major_version"] == 6 and os["minor_version"] == 0 and os["arch"] == "i386":
            return ovirtsdk4.types.OperatingSystem("Windows2008")
        elif os["type"] == "windows" and os["major_version"] == 6 and os["minor_version"] == 0 and os["arch"] == "X86_64":
            return ovirtsdk4.types.OperatingSystem("Windows2008x64")
        elif os["type"] == "windows" and os["major_version"] == 6 and os["minor_version"] == 1 and os["arch"] == "i386":
            return ovirtsdk4.types.OperatingSystem("Windows7")
        elif os["type"] == "windows" and os["major_version"] == 6 and os["minor_version"] == 1 and os["arch"] == "X86_64" and os["product_variant"] == "Client":
            return ovirtsdk4.types.OperatingSystem("Windows7x64")
        elif os["type"] == "windows" and os["major_version"] == 6 and os["minor_version"] == 1 and os["arch"] == "x86_64":
            return ovirtsdk4.types.OperatingSystem("Windows2008R2x64")
        elif os["type"] == "windows" and os["major_version"] == 6 and os["minor_version"] == 2 and os["arch"] == "i386":
            return ovirtsdk4.types.OperatingSystem("windows_8")
        elif os["type"] == "windows" and os["major_version"] == 6 and os["minor_version"] == 2 and os["arch"] == "X86_64" and os["product_variant"] == "Client":
            return ovirtsdk4.types.OperatingSystem("windows_8x64")
        elif os["type"] == "windows" and os["major_version"] == 6 and os["minor_version"] == 2 and os["arch"] == "x86_64":
            return ovirtsdk4.types.OperatingSystem("windows_2012x64")
        elif os["type"] == "windows" and os["major_version"] == 6 and os["minor_version"] == 3 and os["arch"] == "i386" and os["product_varian"] == "Client":
            return ovirtsdk4.types.OperatingSystem("windows_8")
        elif os["type"] == "windows" and os["major_version"] == 6 and os["minor_version"] == 3 and os["arch"] == "X86_64" and os["product_variant"] == "Client":
            return ovirtsdk4.types.OperatingSystem("windows_8x64")
        elif os["type"] == "windows" and os["major_version"] == 6 and os["minor_version"] == 3 and os["arch"] == "x86_64":
            return ovirtsdk4.types.OperatingSystem("windows_2012x64")
        elif os["type"] == "windows" and os["major_version"] == 10 and os["minor_version"] == 0 and os["arch"] == "i386":
            return ovirtsdk4.types.OperatingSystem("windows_10")
        elif os["type"] == "windows" and os["major_version"] == 10 and os["minor_version"] == 0 and os["arch"] == "X86_64" and os["product_variant"] == "Client":
            return ovirtsdk4.types.OperatingSystem("windows_10x64")
        elif os["type"] == "windows" and os["major_version"] == 10 and os["minor_version"] == 0 and os["arch"] == "x86_64":
            return ovirtsdk4.types.OperatingSystem("windows_2016x64")
        else:
            logging.warning("Unsupported architecture %s for %s %d.%d" % (
                os["arch"], os["distro"],
                os["major_version"], os["minor_version"]
            ))
            return ovirtsdk4.types.OperatingSystem("Unassigned")


    def create_vm(self, hardware, operating_systems):
        vms_service = self._connection.system_service().vms_service()

        new_vm = vms_service.add_from_scratch(
            ovirtsdk4.types.Vm(
                name = self._request["vm_name"],
                cluster = ovirtsdk4.types.Cluster(
                    name = self._request["destination"]["cluster"]
                ),
                template = ovirtsdk4.types.Template(
                    name = 'Blank'
                ),
                disk_attachments = [],
                cpu = ovirtsdk4.types.Cpu(
                    topology = ovirtsdk4.types.CpuTopology(
                        cores = hardware["cpu"]["total_cores"],
                        sockets = hardware["cpu"]["sockets"] / hardware["cpu"]["cores_per_socket"],
                        threads = 1
                    )
                ),
                memory = hardware["memory"],
                type = self._best_fit_vm_type(operating_systems[0]),
                os = self._best_fit_os_type(operating_system[0])
            )
        )

        for i in range(1, 60):
            new_vm = self._find_vm_by_id(new_vm.id)
            if new_vm.status == ovirtsdk4.types.VmStatus.DOWN:
                break
            time.sleep(15)

        if new_vm.status != ovirtsdk4.types.VmStatus.DOWN:
            raise Exception("Vm %s is not ready after 15 minute" % self._request["vm_name"])

        for disk_id in vm_spec["disks"]:
            self.attach_disk_to_vm(new_vm.id, disk_id)
        for nic_spec in vm_spec["nics"]:
            self.add_nic_to_vm(new_vm.id, nic_spec)

        return new_vm.id


    def get_local_disk_path(self, disk_spec):
        pass


    def create_disk(self, disk_spec):
        logging.debug("Creating disk with spec: %s", disk_spec)
        destination_disk = disk_spec.copy()
        disks_service = self._connection.system_service().disks_service()
        new_disk = disks_service.add(
            ovirtsdk4.types.Disk(
                name = destination_disk["name"],

                # For disk format, we always use QCOW2, because it allows both
                # sparse and preallocated with all storage backend types. Also,
                # QCOW2 is the only format that supports incremental backups,
                # so we think it's a better choice for customers.
                format = ovirtsdk4.types.DiskFormat.COW,
                sparse = disk_spec["is_sparse"],
                provisioned_size = int(destination_disk["size"]),
                storage_domains = [
                    ovirtsdk4.types.StorageDomain(
                        name = destination_disk["storage_name"]
                    )
                ]
            )
        )

        for i in range(1, 60):
            new_disk = self._find_disk_by_id(new_disk.id)
            if new_disk.status == ovirtsdk4.types.DiskStatus.OK:
                break
            time.sleep(15)

        if new_disk.status != ovirtsdk4.types.DiskStatus.OK:
            raise Exception("Disk %s is not ready after 15 minute" % disk["name"])

        logging.debug("Disk created with id %s", new_disk.id)
        return new_disk.id


    def attach_disk_to_vm(self, vm_id, disk_id):
        disk_attachments_service = self._connection.system_service().vms_service().vm_service(vm_id).disk_attachments_service()
        disk_attachments_service.add(
            ovirtsdk4.types.DiskAttachment(
                disk = self._find_disk_by_id(disk_id),
                interface = ovirtsdk4.types.DiskInterface.VIRTIO_SCSI,
                bootable = False,
                active = True
            )
        )


    def detach_disk_from_vm(self, vm_id, disk_id):
        disk_attachments_service = self._connection.system_service().vms_service().vm_service(vm_id).disk_attachments_service()
        disk_attachment = next(
            (a for a in disk_attachments_service.list() if a.disk.id == disk_id),
            None
        )

        if disk_attachment is None:
            return
        disk_attachment_service = disk_attachments_service.attachment_service(disk_attachment.id)

        disk_attachment_service.update(ovirtsdk4.types.DiskAttachment(active = False))
        for i in range(1, 120):
            if not disk_attachment_service.get().active:
                break
            time.sleep(5)

        if disk_attachment_service.get().active:
           raise Exception("Disk attachment for disk '%s' is still active after 10 minutes" % disk_id)

        disk_attachment_service.remove()
        for i in range(1, 120):
            try:
                disk_attachment_service.get()
                time.sleep(5)
            except ovirtsdk4.NotFoundError:
                break

        try:
            disk_attachment_service.get()
            raise Exception("Disk attachment for disk '%s' has not been deleted after 10 minutes" % disk_id)
        except ovirtsdk4.NotFoundError:
            pass


    def enable_change_block_tracking(self, vm_id):
        pass


    def sync_disks(self, vm_id, disks_mapping, guestfs_helper, sync_index, last_sync):
        pass


    def add_nic_to_vm(self, vm_id, nic_spec):
        vm = self._find_vm_by_id(vm_id)

        data_centers_service = self._connection.system_service().data_centers_service()
        data_center = data_centers_service.list(search="Clusters.name=%s" % self._request["destination"]["cluster"])[0]
        networks_service = data_centers_service.service(data_center.id).networks_service()
        network = next(
            (n for n in networks_service.list() if n.name == nic_spec["network"]),
            None
        )
        if network is None:
            raise Exception("Network '%s' not found in datacenter '%s'" % (nic_spec["network"], data_center.name))

        vnic_profiles = []
        vnic_profiles_service = self._connection.system_service().vnic_profiles_service()
        for vnic_profile in vnic_profiles_service.list():
            if vnic_profile.network.id == network.id:
                vnic_profiles.append(vnic_profile)
        if len(vnic_profiles) != 1:
            raise Exception("Network '%s' has more than one vNIC profile. Unsupported." % nic_spec["network"])

        nics_service = self._connection.system_service().vms_service().vm_service(vm_id).nics_service()
        nics_service.add(
            ovirtsdk4.types.Nic(
                name = nic_spec["name"],
                interface = ovirtsdk4.types.NicInterface("virtio"),
                vnic_profile = ovirtsdk4.types.VnicProfile(id=vnic_profiles[0].id),
                mac = ovirtsdk4.types.Mac(address=nic_spec["mac_address"])
            )
        )
            

