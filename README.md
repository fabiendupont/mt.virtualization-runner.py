[![Coverage Status](https://coveralls.io/repos/github/fdupont-redhat/mt.virtualization-runner/badge.svg?branch=master)](https://coveralls.io/github/fdupont-redhat/mt.virtualization-runner?branch=master)

# Conversion-as-a-Service

This project aims at providing a set of tools to add self service for
conversions. A conversion host is a physical or virtual machine running
virt-v2v to convert the virtual machines. The toolset will make its
consumption easier.

```
# subscription-manager repos \
    --enable='rhel-7-server-rh-common-rpms' \
    --enable='rhel-7-server-openstack-12-tools-rpms' \
    --enable='rhel-server-rhscl-7-beta-rpms'
```

```
# yum install systemd-python
# yum install python-ovirt-engine-sdk4
# yum install python-keystoneauth1 python-novaclient \
              python-cinderclient python-neutronclient
```



