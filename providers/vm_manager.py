from importlib import import_module

class VmManager:

    def __init__(self, request, side, state):
        self._vm_manager = import_module("providers.%s.vm_manager" % request[side]["type"]).VmManager(request, side, state)


    def get_vm_hardware(self, vm_id):
        return self._vm_manager.get_vm_hardware(vm_id)


    def create_vm(self, vm_spec):
        return self._vm_manager.create_vm(vm_spec)


    def create_snapshot(self, vm_id, name=None, description=None, memory=False, quiesce=False):
        return self._vm_manager.create_snapshot(vm_id, name, description, memory, quiesce)


    def remove_snapshot(self, vm_id, snapshot_name):
        self._vm_manager.remove_snapshot(vm_id, snapshot_name)


    def remove_all_snapshots(self, vm_id):
        self._vm_manager.remove_all_snapshots(vm_id)


    def get_local_disk_path(self, disk_spec):
        return self._vm_manager.get_local_disk_path(disk_spec)

    def create_disk(self, disk_spec):
        return self._vm_manager.create_disk(disk_spec)


    def attach_disk_to_vm(self, vm_id, disk_id):
        self._vm_manager.attach_disk_to_vm(vm_id, disk_id)


    def detach_disk_from_vm(self, vm_id, disk_id):
        self._vm_manager.detach_disk_from_vm(vm_id, disk_id)


    def enable_change_block_tracking(self, vm_id):
        self._vm_manager.enable_change_block_tracking(vm_id)


    def sync_disks(self, vm_id, disks_mappings, guestfs_helper, sync_index, last_sync):
        self._vm_manager.sync_disks(vm_id, disks_mappings, guestfs_helper, sync_index, last_sync)


    def add_nic_to_vm(self, vm_id, nic_spec):
        self._vm_manager.add_nic_to_vm(vm_id, nic_spec)

