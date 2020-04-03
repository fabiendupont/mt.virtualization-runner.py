from keystoneauth1 import identity as keystone_identity
from keystoneauth1 import session as keystone_session
from novaclient import client as nova_client
from cinderclient import client as cinder_client
from cinderclient.v2 import volume_transfers as cinder_volume_transfers
from neutronclient.v2_0 import client as neutron_client

class VmManager:
    def __init__(self, request, side, state):
        self._request = request
        self._side = side
        self._state = state
        self._connection = self._connect()
        self._init_os_clients(self._connection)


    def __del__(self):
        self._disconnect()


    def _connect(self):
        connection = {
            "source": self._get_source_keystone_session,
            "destination": self._get_destination_keystone_session
        }
        return connection


    def _disconnect(self):
        pass


    def _get_source_keystone_session(self):
        auth = keystone_identity.Password(
            auth_url       = self._request['destination']['authentication']['url'],
            username       = self._request['destination']['authentication']['username'],
            password       = self._request['destination']['authentication']['password'],
            user_domain_id = self._request['destination']['authentication']['domain'],
            tenant_id=self._request['conversion_host_project']
        )
        return keystone_session.Session(auth=auth)


    def _get_destination_keystone_session(self):
        auth = keystone_identity.Password(
            auth_url       = self._request['destination']['authentication']['manager']['url'],
            username       = self._request['destination']['authentication']['manager']['username'],
            password       = self._request['destination']['authentication']['manager']['password'],
            user_domain_id = self._request['destination']['authentication']['manager']['domain'],
            tenant_id=self._request['destination']['project']
        )
        return keystone_session.Session(auth=auth)


    def _init_os_clients(self, connection):
        source_ks = connection["source"]
        destination_ks = connection["destination"]
        self._source_cinder = cinder_client.Client("3", session=source_ks)
        self._destination_cinder = cinder_client.Client("3", session=destination_ks)
        self._source_neutron = neutron_client.Client(session=source_ks)
        self._destination_neutron = neutron_client.Client(session=destination_ks)
        self._destination_nova = nova_client.Client("2", session=destination_ks)


    def _transfer_volume(self, volume_id):
        transfer_request = cinder_volume_transfers.VolumeTransferManager(self._source_cinder).create(volume_id)
        cinder_volume_transfers.VolumeTransferManager(self._destination_cinder).accept(transfer_request.id, transfer_request.auth_key)


    def _create_network_ports(self, nics):
        network_ports = []
        port_number = 1
        for nic in nics:
            network_port_spec = {
                "name": "%s_port_%d" % (self.request["vm_name"], port_number),
                "admin_state_up": True,
                "network_id": nic["network_id"],
                "mac_address": nic["mac_address"],
                "project_id": self._request["destination"]["project"]
            }
            if "fixed_ip" in nic:
                network_port_spec["fixed_ip"] = nic["ip_address"]
            network_port = self._source_neutron.create_port({ "port": network_port_spec })
            network_ports.append({ "port-id": network_port["port"]["id"] })
        return network_ports


    def _create_block_device_mapping(self, disks):
        block_device_mapping = []
        boot_index = 0
        disk_index = ord("a")
        for disk in disks:
            self._transfer_volume(disk["id"])
            block_device = {
                "device_name": "vd%s" % chr(disk_index),
                "source_type": "volume",
                "destination_type": "volume",
                "delete_on_termination": False,
                "uuid": disk["id"],
                "boot_index": boot_index
            }
            block_device_mapping.append(block_device)
            boot_index += 1
            disk_index += 1
        return block_device_mapping


    def get_vm_hardware(self):
        pass


    def create_vm(self, vm_spec):
        print("Disks: %s" % disks)
        block_device_mapping = self._create_block_device_mapping(vm_spec["disks"])
        network_ports = self._create_network_ports(vm_spec["nics"])
        self._destination_nova.servers.create(
            self._request["vm_name"],
            image=None,
            block_device_mapping_v2=block_device_mapping,
            flavor=self._request["destination"]["flavor"],
            security_groups=self._request["destination"]["security_groups"],
            nics=self._create_network_ports(),
        )


    def create_disk(self, disk_spec):
        pass


    def attach_disk_to_vm(self, vm_id, disk_id):
        pass


    def detach_disk_from_vm(self, vm_id, disk_id):
        pass


    def enable_change_block_tracking(self, vm_id):
        pass


    def sync_disks(self, vm_id, disks_mapping, guestfs_helper, sync_index, last_sync):
        pass


    def add_nic_to_vm(self, vm_id, nic_spec):
        pass


