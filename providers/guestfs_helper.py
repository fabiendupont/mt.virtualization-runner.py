import os
import logging
import subprocess
from importlib import import_module

class GuestfsHelper:
    def __init__(self, request, side, state):
        self._request = request
        self._side = side
        self._state = state
        self._guestfs_helper = import_module("providers.%s.guestfs_helper" % request[side]["type"]).GuestfsHelper(request, side, state)


    def get_vm_operating_system(self, vm_hardware, snapshot_moref=None, extended=False):
        return self._guestfs_helper.get_vm_operating_system(vm_hardware, snapshot_moref, extended)

    def _generate_libvirt_xml(self, vm_hardware):
        from xml.etree import ElementTree as ET
        from xml.dom import minidom

        xml = ET.Element("domain", { "type": "kvm" })
        xml.set("version", "1.0")
        xml.set("encoding", "utf-8")

        xml_name = ET.SubElement(xml, "name")
        xml_name.text = self._request["vm_name"]
        xml_memory = ET.SubElement(xml, "memory")
        xml_memory.text = str(vm_hardware["memory"])
        xml_vcpu = ET.SubElement(xml, "vcpu")
        xml_vcpu.text = str(vm_hardware["vcpu"])

        xml_os = ET.SubElement(xml, "os")
        xml_os_type = ET.SubElement(xml_os, "type")
        xml_os_type.text = "hvm"
        ET.SubElement(xml_os, "boot", { "dev": "hd" })

        xml_features = ET.SubElement(xml, "features")
        ET.SubElement(xml_features, "acpi")
        ET.SubElement(xml_features, "apic")
        ET.SubElement(xml_features, "pae")

        xml_devices = ET.SubElement(xml, "devices")

        disk_index = ord('a')
        for disk_path in vm_hardware["disks"]:
            xml_disk = ET.SubElement(xml_devices, "disk", { "type": "block", "device": "disk" })
            ET.SubElement(xml_disk, "driver", { "name": "qemu", "type": "raw" })
            ET.SubElement(xml_disk, "source", { "dev": disk_path })
            ET.SubElement(xml_disk, "target", { "dev": "sd%s" % chr(disk_index), "bus": "virtio" })
            disk_index += 1

        for nic in vm_hardware["nics"]:
            xml_nic = ET.SubElement(xml_devices, "interface", { "type": "bridge" })
            ET.SubElement(xml_nic, "source", { "bridge": nic["network"] })
            ET.SubElement(xml_nic, "model", { "type": "virtio" })
            ET.SubElement(xml_nic, "mac", { "address": nic["mac_address"] })

        xml_video = ET.SubElement(xml_devices, "video")
        ET.SubElement(xml_video, "model", { "type": "qxl", "ram":"65536", "heads":"1" })

        return minidom.parseString(ET.tostring(xml, "utf-8")).toprettyxml(indent="    ")


    def nbd_process_aio_requests(self, nbd_handle):
        self._guestfs_helper.nbd_process_aio_requests(nbd_handle)


    def nbd_wait_for_aio_commands_to_finish(self, nbd_handle):
        self._guestfs_helper.nbd_wait_for_aio_commands_to_finish(nbd_handle)


    def nbd_expose_disk_cmd(self, disk_spec, **kwargs):
        return self._guestfs_helper.nbd_expose_disk_cmd(disk_spec, **kwargs)


    def convert_vm(self, vm_hardware):
        # Generate the metadata for the destination VM
        metadata_file = "/tmp/%s/metadata.xml" % self._request["vm_name"]
        log_file = "/tmp/%s/virt-v2v.log" % self._request["vm_name"]
        destination_libvirt_xml = self._generate_libvirt_xml(vm_hardware)
        logging.debug(destination_libvirt_xml)
        with open(metadata_file, 'w') as f:
            f.write(destination_libvirt_xml)

        virtv2v_cmd = [
            "env", "LIBGUESTFS_BACKEND=direct",
            "virt-v2v", "--verbose",
            "-i", "libvirtxml", metadata_file,
            "--in-place"
        ]

        with open(log_file, 'w') as log:
            proc = subprocess.Popen(
                virtv2v_cmd,
                stdout=log,
                stderr=subprocess.STDOUT,
                text=True
            )
            return proc
