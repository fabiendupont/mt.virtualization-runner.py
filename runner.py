#!/usr/bin/env python3

import os
import sys
import json
import asyncio
import logging
from datetime import datetime, timezone

from state import State
from request_validator import RequestValidator
from providers.vm_manager import VmManager
from providers.guestfs_helper import GuestfsHelper


class ConversionHostRunner:

    VERSION = "2.0"
    LOG_LEVEL = logging.DEBUG
    SYNC_INTERVAL = 60 # 1 minute

    def __init__(self, request):
        self._request = request

        # Validate that request contains the mandatory info
        request_errors = RequestValidator(self._request).validate()
        if request_errors:
            msg = "Errors in request:\n%s" % "\n".join([" - %s" % e for e in request_errors])
            raise Exception(msg)

        # Create work directory structure if it doesn't exist
        self._work_dir = "/tmp/%s" % self._request["vm_name"]

        if not os.path.isdir(self._work_dir):
            os.makedirs(self._work_dir)

        if not os.path.isdir("%s/disks" % self._work_dir):
            os.mkdir("%s/disks" % self._work_dir)

        if not os.path.isdir("%s/nbd" % self._work_dir):
            os.mkdir("%s/nbd" % self._work_dir)

        # Store start datetime as timestamp
        self._conversion_time = datetime.now(timezone.utc)
        self._conversion_timestamp = self._conversion_time.strftime('%Y%m%dT%H%M%S')

        # Configure logging
        self._log_dir = "/var/log/conversion-host/%s" % self._request["vm_name"]

        if not os.path.isdir(self._log_dir):
            os.makedirs(self._log_dir)

        self._log_file = "%s/%s" % (self._log_dir, self._conversion_timestamp)
        log_format = '%(asctime)s:%(levelname)s: %(message)s (%(module)s:%(lineno)d)'
        logging.basicConfig(filename=self._log_file, format=log_format, level=self.LOG_LEVEL)

        logging.info("Conversion Host Runner version %s, uid=%s", self.VERSION, os.getuid())

        # Generate filenames for state, limits and cutover
        self._state_file = "%s/%s.state" % (self._work_dir, self._conversion_timestamp)
        logging.info("State file: %s", self._state_file)
        self._limits_file = "%s/%s.limits" % (self._work_dir, self._conversion_timestamp)
        logging.info("Limits file: %s", self._limits_file)
        self._cutover_file = "%s/%s.cutover" % (self._work_dir, self._conversion_timestamp)
        logging.info("Cutover file: %s", self._cutover_file)

        open(self._cutover_file, 'a').close()

        # Initialize state
        self._state = State(self._state_file)


    async def _sleep_coroutine(self, delay):
        await asyncio.sleep(delay)


    def run(self):
        try:
            # Instantiate source VmManager
            source_vm_manager = VmManager(self._request, "source", self._state)

            # Instantiate destination VmManager
            destination_vm_manager = VmManager(self._request, "destination", self._state)

            # Instantiate source GuestfsHelper
            source_guestfs_helper = GuestfsHelper(self._request, "source", self._state)

            # Instantiate destination GuestfsHelper
            destination_guestfs_helper = GuestfsHelper(self._request, "destination", self._state)

            # Enable CBT for the source VM
            source_vm_manager.enable_change_block_tracking(self._request["vm_uuid"])

            # Retrieve source VM hardware
            source_vm_hardware = source_vm_manager.get_vm_hardware(self._request["vm_uuid"])
            logging.debug("Source VM Hardware:\n%s", json.dumps(source_vm_hardware, sort_keys=True, indent=4, separators=(',', ': ')))

            # Retrieve VM software
            snapshot = source_vm_manager.create_snapshot(self._request["vm_uuid"], 'inspection')
            source_vm_operating_system = source_guestfs_helper.get_vm_operating_system(source_vm_hardware, snapshot_moref=snapshot._moId)
            source_vm_manager.remove_snapshot(self._request["vm_uuid"], 'inspection')
            logging.debug("Source VM Operating Systems:\n%s", json.dumps(source_vm_operating_systems, sort_keys=True, indent=4, separators=(',', ': ')))
            source_vm_operating_systems = source_vm_operating_systems.copy()

            sys.exit(0)

            # Create a list to keep track of source/destination disks
            disks_mappings = []
            destination_disks_index = 1
            for source_disk in source_vm_hardware["disks"]:
                self._state.disks[source_disk["id"]] = {
                    "path": source_disk["path"],
                    "syncs": []
                }
                destination_disk = {
                    "name": "%s_Disk%s" % (self._request["vm_name"], destination_disks_index),
                    "storage_name": self._request["mappings"]["storage"][source_disk["storage_name"]],
                    "size": source_disk["size"],
                    "format": destination_vm_manager.best_fit_disk_format["storage_name"],
                    "allocation": desintation_vm_manager.best_fit_disk_allocation["storage_name"]
                }
                destination_disk["id"] = destination_vm_manager.create_disk(destination_disk)
                disks_mappings.append({
                    "source": source_disk,
                    "destination": destination_disk
                })
                destination_disks_index = destination_disks_index + 1
            self._state.disk_count = len(source_vm_hardware["disks"])
            self._state.write()

            # Extract and enrich destination disks attributes
            destination_disks_ids = []
            for destination_disk in [dm["destination"] for dm in disks_mappings]:
                destination_disks_ids.append(destination_disk["id"])
                destination_disk["conversion_host_path"] = "/dev/disk/by-id/scsi-0QEMU_QEMU_HARDDISK_%s" % destination_disk["id"][0:20]
            logging.debug(destination_disks_ids)

            # Attach newly created disks to conversion host
            logging.info("Disks attach to conversion host (vm_id=%s)", self._request["conversion_host_uuid"])
            for destination_disk_id in destination_disks_ids:
                destination_vm_manager.attach_disk_to_vm(self._request["conversion_host_uuid"], destination_disk_id)

            # Generate destination VM hardware
            destination_vm_hardware = source_vm_hardware.copy()
            destination_vm_hardware["disks"] = [dm["destination"]["conversion_host_path"] for dm in disks_mappings]
            destination_vm_hardware["nics"] = []
            nic_index = 0
            for nic in source_vm_hardware["nics"]:
                destination_vm_hardware["nics"].append({
                    "name": "nic%d" % nic_index,
                    "mac_address": nic["mac_address"],
                    "network": self._request["mappings"]["network"][nic["network"]]
                })
                nic_index += 1
            logging.debug("Destination VM Hardware:\n%s", json.dumps(destination_vm_hardware, sort_keys=True, indent=4, separators=(',', ': ')))

            # Transfer data
            logging.info("Transferring data...")
            sync_index = 0
            last_sync = False
            while True:
                if os.path.exists(self._cutover_file):
                    logging.info("Cutover is requested. Performing last sync.")
                    last_sync = True
                source_vm_manager.sync_disks(self._request["vm_uuid"], disks_mappings, source_guestfs_helper, sync_index, last_sync)
                if last_sync:
                    break
                sync_index += 1
                asyncio.run(self._sleep_coroutine(self.SYNC_INTERVAL))

            # Convert virtual machine using virt-v2v --in-place
            logging.info("Starting conversion of %s with virt-v2v", self._request["vm_name"])
            virtv2v = destination_guestfs_helper.convert_vm(destination_vm_hardware)
            self._state.pid = virtv2v.pid
            self._state.write()
            virtv2v.wait()
            logging.info("Conversion of %s with virt-v2v completed", self._request["vm_name"])

            # Detach disks from conversion host
            for destination_disk_id in destination_disks_ids:
                destination_vm_manager.detach_disk_from_vm(self._request["conversion_host_uuid"], destination_disk_id)
            destination_vm_hardware["disks"] = destination_disks_ids

            # Create virtual machine from the disks
            logging.debug("Destination VM Hardware: %s", destination_vm_hardware, destination_vm_operating_systems)
            destination_vm_id = destination_vm_manager.create_vm(destination_vm_hardware)
            self._state.vm_id = destination_vm_id

            # Finalize
            self._state.finished = True

        except Exception as err:
            logging.error(err)
            self._state.finished = True
            self._state.failed = True
            self._state.message = err
            raise err

        finally:
            self._state.write()


def main():
    with open(sys.argv[1]) as f:
        request = json.load(f)

    ConversionHostRunner(request).run()


if __name__ == '__main__':
    main()
