from datetime import datetime
import logging
import nbd
import os
import ssl

from pyVmomi import vim
from pyVim.connect import SmartStubAdapter, VimSessionOrientedStub, Disconnect
from pyVim.task import WaitForTask

class VmManager:
    def __init__(self, request, side, state):
        self._request = request
        self._side = side
        self._state = state
        self._connection = self._connect()


    def __del__(self):
        self._disconnect()


    def _connect(self):
        # https://github.com/vmware/pyvmomi/issues/347#issuecomment-297591340
        smart_stub = SmartStubAdapter(
            host = self._request[self._side]["authentication"]["host"]["hostname"],
            port = 443,
            sslContext=ssl._create_unverified_context(),
            connectionPoolTimeout=0
        )
        session_stub = VimSessionOrientedStub(
            smart_stub,
            VimSessionOrientedStub.makeUserLoginMethod(
                self._request[self._side]["authentication"]["host"]["username"],
                self._request[self._side]["authentication"]["host"]["password"],

            )
        )
        conn = vim.ServiceInstance('ServiceInstance', session_stub)

        if not conn:
            raise Exception("Could not connect to vCenter")

        return conn


    def _disconnect(self):
        try:
            Disconnect(self._connection)
        except:
            pass


    def _find_vm_by_id(self, vm_id):
        logging.debug("Looking for virtual machine with UUID '%s'" % vm_id)
        vm = self._connection.content.searchIndex.FindByUuid(None, vm_id, True)
        if vm is None:
            raise Exception("No virtual machine with UUID '%s'" % vm_id)
        return vm


    def _get_hosts(self):
        host_view = self._connection.content.viewManager.CreateContainerView(self._connection.content.rootFolder, [vim.HostSystem], True)
        hosts = [host for host in host_view.view]
        host_view.Destroy()
        return hosts


    def _get_device_controller(self, device, device_list):
        if device.controllerKey is None:
            return None

        ctlr = next(dev for dev in device_list if dev.key == device.controllerKey)
        controller = {
            "type": type(ctlr).__name__.split(".")[-1],
            "key": ctlr.key,
            "name": ctlr.deviceInfo.label,
            "bus_number": ctlr.busNumber,
            "controller": self._get_device_controller(ctlr, device_list)
        }

        if ctlr.slotInfo is not None:
            controller["pci_slot_number"] = ctlr.slotInfo.pciSlotNumber

        return controller


    def get_vm_hardware(self, vm_id):
        vm = self._find_vm_by_id(vm_id)
        host = vm.runtime.host
        content = self._connection.RetrieveContent()

        hardware = {
            "metadata": {
                "vm_moref": vm._moId,
                "has_snapshots": vm.snapshot is not None,
                "cbt_enabled": vm.config.changeTrackingEnabled
            },
            "cpu": {
                "sockets": vm.config.hardware.numCPU / vm.config.hardware.numCoresPerSocket,
                "cores_per_socket": vm.config.hardware.numCoresPerSocket
            },
            "memory": vm.config.hardware.memoryMB * 2**20,
            "disks": [],
            "nics": [],
            "video_cards": []
        }

        device_list = vm.config.hardware.device
        for device in device_list:
            if type(device).__name__ == 'vim.vm.device.VirtualDisk':
                datastore = device.backing.datastore
                datastore_mountpoint = datastore.summary.url.replace("ds://", "")
                datastore_uuid = datastore.summary.url.replace("ds://", "").split("/")[-1]
                absolute_path = "%s/%s" % (datastore_mountpoint, device.backing.fileName.replace("[%s] " % datastore.name, ""))
                disk = {
                    "id": device.backing.uuid,
                    "key": device.key,
                    "name": device.deviceInfo.label,
                    "size": device.capacityInBytes,
                    "path": device.backing.fileName,
                    "absolute_path": absolute_path,
                    "datastore_name": datastore.name,
                    "datastore_mountpoint": datastore_mountpoint,
                    "datastore_uuid": datastore_uuid,
                    "is_sparse": device.backing.thinProvisioned,
                    "is_rdm": type(device.backing).__name__ == 'vim.vm.device.VirtualDisk.VirtualDiskRawDiskMappingVer1BackingInfo',
                    "unit_number": device.unitNumber,
                    "controller": self._get_device_controller(device, device_list)
                }

                hardware["disks"].append(disk)

            # https://github.com/vmware/pyvmomi-community-samples/blob/master/samples/getvnicinfo.py
            if type(device.backing).__name__ == 'vim.vm.device.VirtualEthernetCard.NetworkBackingInfo':
                if hasattr(device.backing, 'port'):
                    dvsUuid = device.backing.port.switchUuid
                    try:
                        dvs = content.dvSwitchManager.QueryDvsByUuid(dvsUuid)
                        network_name = str(dvs.name)
                    except:
                        raise Exception("DVS with UUID '%s' not found" % dvsUuid)
                else:
                    vm_portgroup = device.backing.network.name
                    host_portgroups = host.config.network.portgroup
                    for host_portgroup in host_portgroups:
                        if vm_portgroup in host_portgroup.key:
                            network_name = str(host_portgroup.spec.name)

                nic = {
                    "key": device.key,
                    "name": device.deviceInfo.label,
                    "mac_address": device.macAddress,
                    "network": {
                        "type": "distributed" if hasattr(device.backing, 'port') else "local",
                        "name": device.backing.deviceName
                    },
                    "pci_slot_number": device.slotInfo.pciSlotNumber,
                    "controller": self._get_device_controller(device, device_list)
                }

                hardware["nics"].append(nic)

            if type(device).__name__ == 'vim.vm.device.VirtualVideoCard':
                video_card = {
                    "key": device.key,
                    "name": device.deviceInfo.label,
                    "video_memory": device.videoRamSizeInKB * 1024,
                    "graphics_memory": device.graphicsMemorySizeInKB * 104,
                    "number_of_displays": device.numDisplays,
                    "3d_enabled": device.enable3DSupport,
                    "controller": self._get_device_controller(device, device_list)
                }

                hardware["video_cards"].append(video_card)

        return hardware


    def create_vm(self, vm_spec):
        pass


    def create_disk(self, disk_spec):
        pass


    def attach_disk_to_vm(self, vm_id, disk_id):
        pass


    def detach_disk_from_vm(self, vm_id, disk_id):
        pass


    def enable_change_block_tracking(self, vm_id):
        logging.info("Enable change block tracking for VM: %s" % vm_id)
        vm = self._find_vm_by_id(vm_id)
        config_spec = vim.vm.ConfigSpec(changeTrackingEnabled = True)
        WaitForTask(vm.Reconfigure(config_spec))


    def create_snapshot(self, vm_id, name, description=None, memory=False, quiesce=False):
        logging.info("Create snapshot for VM: %s", vm_id)
        vm = self._find_vm_by_id(vm_id)

        now = datetime.now()
        if not description:
            description = name

        WaitForTask(vm.CreateSnapshot(
            name=name,
            description=description,
            memory=memory,
            quiesce=quiesce
        ))

        return vm.snapshot.currentSnapshot


    def _find_snapshot_by_name(self, snapshots, name):
        for snapshot in snapshots:
            if snapshot.name == name:
                return snapshot
            else:
                return self._find_snapshot_by_name(snapshot.childSnapshotList, name)


    def remove_snapshot(self, vm_id, snapshot_name):
        logging.info("Removing snapshot named '%s' for VM '%s'" % (snapshot_name, vm_id))
        vm = self._find_vm_by_id(vm_id)
        snapshot_tree = self._find_snapshot_by_name(vm.snapshot.rootSnapshotList, snapshot_name)
        if snapshot_tree is not None:
            WaitForTask(snapshot_tree.snapshot.RemoveSnapshot_Task(True))


    def remove_all_snapshots(self, vm_id):
        logging.info("Removing all snapshots for VM: %s" % vm_id)
        vm = self._find_vm_by_id(vm_id)
        WaitForTask(vm.RemoveAllSnapshots())
        logging.info("All snapshots removed for VM: %s" % vm_id)


    def _update_change_ids(self, vm_id, snapshot, disks_mappings):
        logging.info("Getting change ids for VM: %s" % vm_id)
        vm = self._find_vm_by_id(vm_id)
        for device in snapshot.config.hardware.device:
            if type(device).__name__ == 'vim.vm.device.VirtualDisk':
                dm = next(
                    (dm for dm in disks_mappings if dm["source"]["id"] == device.backing.uuid),
                    None
                )
                if not "change_ids" in dm["source"]:
                    dm["source"]["change_ids"] = ["*"]
                if device.backing.changeId:
                    dm["source"]["change_ids"].append(device.backing.changeId)


    def _get_changed_extents(self, vm_id, snapshot, disks_mappings, sync_index, last_sync):
        logging.info("Getting the list of changed extents since previous snapshot")
        vm = self._find_vm_by_id(vm_id)
        for dm in disks_mappings:
            total_len = 0
            extents = []

            logging.debug("Change Ids for disk %s: %s", dm["source"]["id"], dm["source"]["change_ids"])
            change_id = dm["source"]["change_ids"][-2]
            
            logging.debug("Retrieving changed extents for change_id: %s", change_id)
            while total_len < dm["source"]["size"]:
                tmp = vm.QueryChangedDiskAreas(snapshot, int(dm["source"]["key"]), total_len, change_id)
                extents.extend(tmp.changedArea)
                total_len = tmp.startOffset + tmp.length
            total_size = sum(x.length for x in extents)
            logging.debug("Gathered %d extents to transfer, with a total size of %d B" % (len(extents), total_size))
            dm["source"]["extents"] = extents


    def _get_block_status(self, nbd_handle, extent):

        MAX_BLOCK_STATUS_LEN = 2 << 30 # 2GB (4GB requests fail over the 32b protocol)

        logging.debug("Gathering block status for extent of size %d B at offset %d B" % (extent.length, extent.start))

        blocks = []
        last_offset = extent.start

        def update_blocks(metacontext, offset, extents, err):
            logging.debug("Update blocks - Metacontext: %s - Offset: %s - Extents: %s", metacontext, offset, extents)
            if metacontext != "base:allocation":
                return
            for length, flags in zip(extents[::2], extents[1::2]):
                blocks.append({
                    "offset": offset,
                    "length": length,
                    "flags": flags
                })
                offset += length

        while last_offset < extent.start + extent.length:
            nblocks = len(blocks)
            length = min(extent.length, MAX_BLOCK_STATUS_LEN)
            logging.debug("Calling block_status with length=%d offset=%d" % (length, last_offset))
            nbd_handle.block_status(length, last_offset, update_blocks)
            if len(blocks) == nblocks:
                raise Exception("Missing block status data from NBD")
            last_offset = blocks[-1]['offset'] + blocks[-1]['length']

        return blocks


    def _write_data(self, fd, buf, offset, err):
        logging.debug("Writing %d B to offset %d B", buf.size(), offset)
        os.pwrite(fd, buf.to_bytearray(), offset)
        # By returning 1 here, we auto-retire the aio_pread command
        return 1


    def sync_disks(self, vm_id, disks_mappings, guestfs_helper, sync_index, last_sync):
        MAX_PREAD_LEN = 23 << 20 # 23MB (24MB requests fail in VDDK)
        MAX_AIO_IN_FLIGHT = 2

        vm = self._find_vm_by_id(vm_id)
        snapshot = self.create_snapshot(vm_id, name="conversion-%s" % now.strftime('%Y%m%d-%H%M%S'))
        self._update_change_ids(vm_id, snapshot, disks_mappings)
        self._get_changed_extents(vm_id, snapshot, disks_mappings, sync_index, last_sync)

        for dm in disks_mappings:
            logging.debug("Opening locally attached disk %s" % dm["destination"]["conversion_host_path"])
            fd = os.open(dm["destination"]["conversion_host_path"], os.O_WRONLY | os.O_CREAT)

            if len(dm["source"]["extents"]) == 0:
                os.close(fd)
                continue

            logging.info("Connecting the source disk %s with NBD", dm["source"]["id"])
            nbd_cmd = guestfs_helper.nbd_expose_disk_cmd(dm["source"], None, vm_moref=vm._moId, sync_index=sync_index)
            logging.debug("NBD Command: %s", nbd_cmd)
            nbd_handle = nbd.NBD()
            nbd_handle.add_meta_context("base:allocation")
            nbd_handle.connect_command(nbd_cmd)

            logging.info("Getting block info for disk: %s" % dm["source"]["id"])
            copied = 0
            position = 0
            data_blocks = []
            for extent in dm["source"]["extents"]:
                if extent.length < 1 << 20:
                    logging.debug("Skipping block status for extent of size %d B at offset %d B" % (extent.length, extent.start))
                    data_blocks.append({
                        "offset": extent.start,
                        "length": extent.length,
                        "flags": 0
                    })
                    continue

                blocks = self._get_block_status(nbd_handle, extent)
                logging.debug("Gathered block status of %d: %s" % (len(blocks), blocks))
                data_blocks += [x for x in blocks if not x['flags'] & nbd.STATE_HOLE]

            logging.debug("Block status filtered down to %d data blocks" % len(data_blocks))
            if len(data_blocks) == 0:
                logging.debug("No extents have allocated data for disk: %s" % dm["source"]["id"])
                os.close(fd)
                continue

            to_copy = sum([x['length'] for x in data_blocks])
            logging.debug("Copying %d B of data for disk %s" % (to_copy, dm["source"]["id"]))

            self._state.disks[dm["source"]["id"]]["syncs"].append({
                "to_copy": to_copy,
                "copied": 0
            })
            self._state.write()

            for block in data_blocks:
                logging.debug("Block at offset %s flags: %s", block["offset"], block["flags"])
                if block["flags"] & nbd.STATE_ZERO:
                    logging.debug("Writing %d B of zeros to offset %d B" % (block["length"], block["offset"]))
                    # Optimize for memory usage, maybe?
                    os.pwrite(fd, [0] * block["length"], block["offset"])
                else:
                    count = 0
                    while count < block["length"]:
                        length = min(block["length"] - count, MAX_PREAD_LEN)
                        offset = block["offset"] + count

                        logging.debug("Reading %d B from offset %d B" % (length, offset))
                        buf = nbd.Buffer(length)
                        nbd_handle.aio_pread(
                            buf, offset,
                            lambda err, fd=fd, buf=buf, offset=offset: self._write_data(fd, buf, offset, err)
                        )
                        count += length

                        while nbd_handle.aio_in_flight() > MAX_AIO_IN_FLIGHT:
                            nbd_handle.poll(-1)
                        guestfs_helper.nbd_process_aio_requests(nbd_handle)

                        copied += length
                        self._state.disks[dm["source"]["id"]]["syncs"][sync_index]["copied"] = copied
                        self._state.write()
                
            guestfs_helper.nbd_wait_for_aio_commands_to_finish(nbd_handle)

            if copied == 0:
                logging.debug("Nothing to copy for disk: %s" % dm["source"]["id"])
            else:
                logging.debug("Copied %d B for disk: %s" % (copied, dm["source"]["id"]))

            nbd_handle.shutdown()
            os.close(fd)

        self._remove_all_snapshots(vm_id)


    def add_nic_to_vm(self, vm_id, nic_spec):
        pass
