#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

from neutron_lib import exceptions as n_exc
from oslo_log import log
import retrying
import six
import netaddr
from urlparse import urlparse

from ovs.stream import Stream

from networking_ovn import ovn_db_sync
from neutron.agent.ovsdb import impl_idl
from neutron.agent.ovsdb.native import connection
from neutron.agent.ovsdb.native import idlutils
from neutron.common import utils as n_utils
from neutron import manager

from networking_ovn._i18n import _, _LI
from networking_ovn.common import config as cfg
from networking_ovn.common import constants as ovn_const
from networking_ovn.common import utils
from networking_ovn.ovsdb import commands as cmd
from networking_ovn.ovsdb import ovn_api
from networking_ovn.ovsdb import ovsdb_monitor
from networking_ovn.ovsdb_ha.ovsdb_leader import OVSDBWatchLeaderThread

LOG = log.getLogger(__name__)


class OvsdbConnectionUnavailable(n_exc.ServiceUnavailable):
    message = _("OVS database connection to %(db_schema)s failed with error: "
                "'%(error)s'. Verify that the OVS and OVN services are "
                "available and that the 'ovn_nb_connection' and "
                "'ovn_sb_connection' configuration options are correct.")


# Retry forever to get the OVN NB and SB IDLs. Wait 2^x * 1 seconds between
# each retry, up to 180 seconds, then 180 seconds afterwards.
def get_ovn_idls(driver, trigger):
    @retrying.retry(wait_exponential_multiplier=1000,
                    wait_exponential_max=(180 * 1000))
    def get_ovn_idl_retry(cls, driver, trigger):
        LOG.info(_LI('Getting %(cls)s for %(trigger)s with retry'),
                 {'cls': cls.__name__, 'trigger': trigger.im_class.__name__})
        return cls(driver, trigger)

    nb_ovn_idl = get_ovn_idl_retry(OvsdbNbOvnIdl, driver, trigger)
    sb_ovn_idl = get_ovn_idl_retry(OvsdbSbOvnIdl, driver, trigger)
    return nb_ovn_idl, sb_ovn_idl


def get_connection(db_class, trigger=None, leader_info=None):
    # The trigger is the start() method of the NeutronWorker class
    if trigger and trigger.im_class == ovsdb_monitor.OvnWorker:
        cls = ovsdb_monitor.OvnConnection
    else:
        cls = connection.Connection

    Stream.ssl_set_private_key_file(cfg.get_ovn_ovsdb_private_key_file())
    Stream.ssl_set_certificate_file(cfg.get_ovn_ovsdb_certificate_file())
    Stream.ssl_set_ca_cert_file(cfg.get_ovn_ovsdb_ca_cert_file())

    if db_class == OvsdbNbOvnIdl:
        if leader_info is None:
            leader_info = cfg.get_ovn_nb_connection()
        return cls(leader_info,
                   cfg.get_ovn_ovsdb_timeout(), 'OVN_Northbound')
    elif db_class == OvsdbSbOvnIdl:
        return cls(cfg.get_ovn_sb_connection(),
                   cfg.get_ovn_ovsdb_timeout(), 'OVN_Southbound')


class OvsdbNbOvnIdl(ovn_api.API):
    ovsdb_connection = None
    ovsdb_leader_thread = None
    leader_info = None

    def __init__(self, driver, trigger=None):
        super(OvsdbNbOvnIdl, self).__init__()
        self.driver, self.trigger = driver, trigger
        self.ovsdb_timeout = cfg.get_ovn_ovsdb_timeout()

        if OvsdbNbOvnIdl.ovsdb_leader_thread is None:
            hosts = ()
            for etcd_connect in cfg.get_ovn_etcd_connection():
                url = urlparse(etcd_connect)
                hosts = hosts + ((url.hostname, url.port),)
            if len(hosts) > 0:
                thread = OVSDBWatchLeaderThread(hosts)
                thread.setDaemon(True)
                thread.start()
                OvsdbNbOvnIdl.leader_info = thread.get_ovsdb_leader()
                OvsdbNbOvnIdl.ovsdb_leader_thread = thread

        if OvsdbNbOvnIdl.ovsdb_connection is None:
            self.__init_connect__(OvsdbNbOvnIdl.leader_info)
        self.idl = OvsdbNbOvnIdl.ovsdb_connection.idl
        if OvsdbNbOvnIdl.ovsdb_leader_thread is not None:
            OvsdbNbOvnIdl.ovsdb_leader_thread.registered(self)

    def __init_connect__(self, leader_info=None):
        try:
            LOG.info('Start connect %s.', leader_info)
            OvsdbNbOvnIdl.ovsdb_connection = get_connection(
                OvsdbNbOvnIdl, self.trigger, leader_info)
            if isinstance(OvsdbNbOvnIdl.ovsdb_connection,
                          ovsdb_monitor.OvnConnection):
                OvsdbNbOvnIdl.ovsdb_connection.start(self.driver)
            else:
                OvsdbNbOvnIdl.ovsdb_connection.start()
        except Exception as e:
            connection_exception = OvsdbConnectionUnavailable(
                db_schema='OVN_Northbound', error=e)
            LOG.exception(connection_exception)
            raise connection_exception

    def __reconnect__(self, leader_info=None):
        if leader_info != OvsdbNbOvnIdl.leader_info:
            OvsdbNbOvnIdl.ovsdb_connection.stop()
            self.__init_connect__(leader_info)
            OvsdbNbOvnIdl.leader_info = leader_info

            self.idl = OvsdbNbOvnIdl.ovsdb_connection.idl
            self.nb_synchronizer = ovn_db_sync.OvnNbSynchronizer(
                manager.NeutronManager.get_plugin(),
                self,
                cfg.get_ovn_neutron_sync_mode(),
                self.driver
            )
            self.nb_synchronizer.sync()

    @property
    def _tables(self):
        return self.idl.tables

    def transaction(self, check_error=False, log_errors=True, **kwargs):
        return impl_idl.Transaction(self,
                                    OvsdbNbOvnIdl.ovsdb_connection,
                                    self.ovsdb_timeout,
                                    check_error, log_errors)

    def create_lswitch(self, lswitch_name, may_exist=True, **columns):
        return cmd.AddLSwitchCommand(self, lswitch_name,
                                     may_exist, **columns)

    def delete_lswitch(self, lswitch_name=None, ext_id=None, if_exists=True):
        if lswitch_name is not None:
            return cmd.DelLSwitchCommand(self, lswitch_name, if_exists)
        else:
            raise RuntimeError(_("Currently only supports delete "
                                 "by lswitch-name"))

    def set_lswitch_ext_id(self, lswitch_id, ext_id, if_exists=True):
        return cmd.LSwitchSetExternalIdCommand(self, lswitch_id,
                                               ext_id[0], ext_id[1],
                                               if_exists)

    def create_lswitch_port(self, lport_name, lswitch_name, may_exist=True,
                            **columns):
        return cmd.AddLSwitchPortCommand(self, lport_name, lswitch_name,
                                         may_exist, **columns)

    def set_lswitch_port(self, lport_name, if_exists=True, **columns):
        return cmd.SetLSwitchPortCommand(self, lport_name,
                                         if_exists, **columns)

    def delete_lswitch_port(self, lport_name=None, lswitch_name=None,
                            ext_id=None, if_exists=True):
        if lport_name is not None:
            return cmd.DelLSwitchPortCommand(self, lport_name,
                                             lswitch_name, if_exists)
        else:
            raise RuntimeError(_("Currently only supports "
                                 "delete by lport-name"))

    def get_all_logical_switches_ids(self):
        result = {}
        for row in self._tables['Logical_Switch'].rows.values():
            result[row.name] = row.external_ids
        return result

    def get_logical_switch_ids(self, lswitch_name):
        for row in self._tables['Logical_Switch'].rows.values():
            if row.name == lswitch_name:
                return row.external_ids
        return {}

    def get_all_logical_switch_ports_ids(self):
        result = {}
        for row in self._tables['Logical_Switch_Port'].rows.values():
            result[row.name] = row.external_ids
        return result

    def get_all_logical_switches_with_ports(self):
        result = []
        for lswitch in self._tables['Logical_Switch'].rows.values():
            if ovn_const.OVN_NETWORK_NAME_EXT_ID_KEY not in (
                lswitch.external_ids):
                continue
            ports = []
            for lport in getattr(lswitch, 'ports', []):
                if ovn_const.OVN_PORT_NAME_EXT_ID_KEY in lport.external_ids:
                    ports.append(lport.name)
            result.append({'name': lswitch.name,
                           'ports': ports})
        return result

    def get_all_logical_routers_with_rports(self):
        """Get logical Router ports associated with all logical Routers

        @return: list of dict, each dict has key-value:
                 - 'name': string router_id in neutron.
                 - 'static_routes': list of static routes dict.
                 - 'ports': dict of port_id in neutron (key) and networks on
                            port (value).
        """
        result = []
        gateways = {}
        lrouters = self._tables['Logical_Router'].rows.values()
        for lrouter in lrouters:
            if ovn_const.OVN_GATEWAY_EXT_ID_KEY in (lrouter.external_ids):
                gateways[lrouter.name] = lrouter

        for lrouter in lrouters:
            if ovn_const.OVN_ROUTER_NAME_EXT_ID_KEY not in (
                    lrouter.external_ids):
                continue

            lrports = {}

            for lrport in getattr(lrouter, 'ports', []):
                if not lrport.name.startswith('lrp-'):
                    continue;
                lrports[lrport.name.replace('lrp-', '')] = lrport.networks

            sroutes = []
            for sroute in getattr(lrouter, 'static_routes', []):
                if not (sroute.ip_prefix == ovn_const.OVN_DEFAULT_ROUTE_CIDR and
                         sroute.nexthop == ovn_const.OVN_GATEWAY_TRANSIT_PORT_IP):
                    sroutes.append({'destination': sroute.ip_prefix, 'nexthop': sroute.nexthop})

            lrouter_name = lrouter.name.replace('neutron-', '')
            gateway = gateways.get('gateway-' + lrouter_name)
            gateway_ports = {}
            floating_ip_nats = []
            next_hop = None
            if gateway:
                for gateway_port in getattr(gateway, 'ports', []):
                    if not gateway_port.name.startswith('lrp-'):
                        continue;
                    gateway_ports[gateway_port.name.replace('lrp-', '')] = gateway_port.networks
                for sroute in getattr(gateway, 'static_routes', []):
                    if sroute.ip_prefix == ovn_const.OVN_DEFAULT_ROUTE_CIDR:
                        next_hop = sroute.nexthop
                for nat in getattr(gateway, 'nat', []):
                    if nat.type == 'dnat_and_snat':
                        floating_ip_nats.append({'floating_ip_address': nat.external_ip,
                                                 'fixed_ip_address': nat.logical_ip})

            result.append({'name': lrouter_name,
                           'static_routes': sroutes,
                           'ports': lrports,
                           'gateway_ports': gateway_ports,
                           'floating_ips': floating_ip_nats,
                           'next_hop': next_hop})
        return result

    def get_logical_routers_rports(self, lrouter_name):
        """Get logical Router ports associated with logical Router

        @param lrouter_name: name (string)
        @return: list of dict, each dict has key-value:
                 - 'name': port name(string).
                 - 'cidr': cidr

        """

        def network_to_cidr(network):
            """ip/mask_len to standard cidr format."""
            ip_string = network.split('/')[0]
            mask_len = network.split('/')[1]
            ip_address = netaddr.ip.IPAddress(ip_string)
            net = netaddr.ip.IPAddress(ip_address.value & (~ 0x01 << 31 - int(mask_len)))
            cidr = str(net) + '/' + mask_len
            return cidr

        result = []
        try:
            lrouter = idlutils.row_by_value(self.idl,
                                    'Logical_Router',
                                    'name',
                                    lrouter_name)
        except idlutils.RowNotFound:
            pass
        for lrport in getattr(lrouter, 'ports', []):
            if not str(lrport.name).startswith('dvr-to-transit-'):
                cidr = network_to_cidr(lrport.networks[0])
                result.append({'name': lrport.name, 'cidr': cidr})
        return result

    def get_acls_for_lswitches(self, lswitch_names):
        """Get the existing set of acls that belong to the logical switches

        @param lswitch_names: List of logical switch names
        @type lswitch_names: []
        @var acl_values_dict: A dictionary indexed by port_id containing the
                              list of acl values in string format that belong
                              to that port
        @var acl_obj_dict: A dictionary indexed by acl value containing the
                           corresponding acl idl object.
        @var lswitch_ovsdb_dict: A dictionary mapping from logical switch
                                 name to lswitch idl object
        @return: (acl_values_dict, acl_obj_dict, lswitch_ovsdb_dict)
        """
        acl_values_dict = {}
        acl_obj_dict = {}
        lswitch_ovsdb_dict = {}
        for lswitch_name in lswitch_names:
            try:
                lswitch = idlutils.row_by_value(self.idl,
                                                'Logical_Switch',
                                                'name',
                                                utils.ovn_name(lswitch_name))
            except idlutils.RowNotFound:
                # It is possible for the logical switch to be deleted
                # while we are searching for it by name in idl.
                continue
            lswitch_ovsdb_dict[lswitch_name] = lswitch
            acls = getattr(lswitch, 'acls', [])

            # Iterate over each acl in a lswitch and store the acl in
            # a key:value representation for e.g. acl_string. This
            # key:value representation can invoke the code -
            # self._ovn.add_acl(**acl_string)
            for acl in acls:
                ext_ids = getattr(acl, 'external_ids', {})
                port_id = ext_ids.get('neutron:lport')
                acl_list = acl_values_dict.setdefault(port_id, [])
                acl_string = {'lport': port_id,
                              'lswitch': utils.ovn_name(lswitch_name)}
                for acl_key in six.iterkeys(getattr(acl, "_data", {})):
                    try:
                        acl_string[acl_key] = getattr(acl, acl_key)
                    except AttributeError:
                        pass
                acl_obj_dict[str(acl_string)] = acl
                acl_list.append(acl_string)
        return acl_values_dict, acl_obj_dict, lswitch_ovsdb_dict

    def create_lrouter(self, name, may_exist=True, **columns):
        return cmd.AddLRouterCommand(self, name,
                                     may_exist, **columns)

    def update_lrouter(self, name, if_exists=True, **columns):
        return cmd.UpdateLRouterCommand(self, name,
                                        if_exists, **columns)

    def delete_lrouter(self, name, if_exists=True):
        return cmd.DelLRouterCommand(self, name, if_exists)

    def add_lrouter_port(self, name, lrouter, **columns):
        return cmd.AddLRouterPortCommand(self, name, lrouter, **columns)

    def update_lrouter_port(self, name, lrouter, if_exists=True, **columns):
        return cmd.UpdateLRouterPortCommand(self, name, lrouter,
                                            if_exists, **columns)

    def delete_lrouter_port(self, name, lrouter, if_exists=True):
        return cmd.DelLRouterPortCommand(self, name, lrouter,
                                         if_exists)

    def set_lrouter_port_in_lswitch_port(self, lswitch_port, lrouter_port):
        return cmd.SetLRouterPortInLSwitchPortCommand(self, lswitch_port,
                                                      lrouter_port)

    def add_acl(self, lswitch, lport, **columns):
        return cmd.AddACLCommand(self, lswitch, lport, **columns)

    def delete_acl(self, lswitch, lport, if_exists=True):
        return cmd.DelACLCommand(self, lswitch, lport, if_exists)

    def update_acls(self, lswitch_names, port_list, acl_new_values_dict,
                    need_compare=True, is_add_acl=True):
        return cmd.UpdateACLsCommand(self, lswitch_names,
                                     port_list, acl_new_values_dict,
                                     need_compare=need_compare,
                                     is_add_acl=is_add_acl)

    def add_nat(self, lrouter, **columns):
        return cmd.AddNatCommand(self, lrouter, **columns)

    def delete_nat(self, lrouter, logical_ip=None, external_ip=None, type=None, if_exists=True):
        return cmd.DelNatCommand(self, lrouter, logical_ip, external_ip, type, if_exists)

    def add_static_route(self, lrouter, **columns):
        return cmd.AddStaticRouteCommand(self, lrouter, **columns)

    def delete_static_route(self, lrouter, ip_prefix, nexthop, if_exists=True):
        return cmd.DelStaticRouteCommand(self, lrouter, ip_prefix, nexthop,
                                         if_exists)

    def create_address_set(self, name, may_exist=True, **columns):
        return cmd.AddAddrSetCommand(self, name, may_exist, **columns)

    def delete_address_set(self, name, if_exists=True, **columns):
        return cmd.DelAddrSetCommand(self, name, if_exists)

    def update_address_set(self, name, addrs_add, addrs_remove,
                           if_exists=True):
        return cmd.UpdateAddrSetCommand(self, name, addrs_add, addrs_remove,
                                        if_exists)

    def update_address_set_ext_ids(self, name, external_ids, if_exists=True):
        return cmd.UpdateAddrSetExtIdsCommand(self, name, external_ids,
                                              if_exists)

    def get_all_chassis_router_bindings(self, chassis_candidate_list=None):
        chassis_bindings = {}
        for chassis_name in chassis_candidate_list or []:
            chassis_bindings.setdefault(chassis_name, [])
        for lrouter in self._tables['Logical_Router'].rows.values():
            if all(r not in (lrouter.external_ids) for r in
                 (ovn_const.OVN_ROUTER_NAME_EXT_ID_KEY,
                  ovn_const.OVN_GATEWAY_EXT_ID_KEY)):
                continue
            chassis_name = lrouter.options.get('chassis')
            if not chassis_name:
                continue
            if (not chassis_candidate_list or
                    chassis_name in chassis_candidate_list):
                routers_hosted = chassis_bindings.setdefault(chassis_name, [])
                routers_hosted.append(lrouter.name)
        return chassis_bindings

    def get_router_chassis_binding(self, router_name):
        try:
            router = idlutils.row_by_value(self.idl,
                                           'Logical_Router',
                                           'name',
                                           router_name)
            chassis_name = router.options.get('chassis')
            if chassis_name == ovn_const.OVN_GATEWAY_INVALID_CHASSIS:
                return None
            else:
                return chassis_name
        except idlutils.RowNotFound:
            return None

    def get_unhosted_routers(self, valid_chassis_list):
        unhosted_routers = {}
        for lrouter in self._tables['Logical_Router'].rows.values():
            if all(r not in (lrouter.external_ids) for r in
                 (ovn_const.OVN_ROUTER_NAME_EXT_ID_KEY,
                  ovn_const.OVN_GATEWAY_EXT_ID_KEY)):
                continue
            chassis_name = lrouter.options.get('chassis')
            if not chassis_name:
                # Not a gateway router
                continue
            # TODO(azbiswas): Handle the case when a chassis is no
            # longer valid. This may involve moving conntrack states,
            # so it needs to discussed in the OVN community first.
            if (chassis_name == ovn_const.OVN_GATEWAY_INVALID_CHASSIS or
                    chassis_name not in valid_chassis_list):
                unhosted_routers[lrouter.name] = lrouter.options
        return unhosted_routers

    def add_dhcp_options(self, subnet_id, port_id=None, may_exists=True,
                         **columns):
        return cmd.AddDHCPOptionsCommand(self, subnet_id, port_id=port_id,
                                         may_exists=may_exists, **columns)

    def delete_dhcp_options(self, row_uuid, if_exists=True):
        return cmd.DelDHCPOptionsCommand(self, row_uuid, if_exists=if_exists)

    def get_subnet_dhcp_options(self, subnet_id):
        for row in self._tables['DHCP_Options'].rows.values():
            external_ids = getattr(row, 'external_ids', {})
            port_id = external_ids.get('port_id')
            if subnet_id == external_ids.get('subnet_id') and not port_id:
                return {'cidr': row.cidr, 'options': dict(row.options),
                        'external_ids': dict(external_ids),
                        'uuid': row.uuid}
        return None

    def get_all_dhcp_options(self):
        dhcp_options = {'subnets': {}, 'ports': {}}

        for row in self._tables['DHCP_Options'].rows.values():
            external_ids = getattr(row, 'external_ids', {})
            if not external_ids.get('subnet_id'):
                # This row is not created by OVN ML2 driver. Ignore it.
                continue

            if not external_ids.get('port_id'):
                dhcp_options['subnets'][external_ids['subnet_id']] = {
                    'cidr': row.cidr, 'options': dict(row.options),
                    'external_ids': dict(external_ids),
                    'uuid': row.uuid}
            else:
                dhcp_options['ports'][external_ids['port_id']] = {
                    'cidr': row.cidr, 'options': dict(row.options),
                    'external_ids': dict(external_ids),
                    'uuid': row.uuid}

        return dhcp_options

    def get_port_dhcp_options(self, subnet_id, port_id):
        for row in self._tables['DHCP_Options'].rows.values():
            external_ids = getattr(row, 'external_ids', {})
            if subnet_id == external_ids.get('subnet_id') and (
                    port_id == external_ids.get('port_id')):
                return {'cidr': row.cidr, 'options': dict(row.options),
                        'external_ids': dict(external_ids),
                        'uuid': row.uuid}
        return None

    def compose_dhcp_options_commands(self, subnet_id, **columns):
        # First add the subnet DHCP options.
        commands = [self.add_dhcp_options(subnet_id, **columns)]

        # Check if there are any port DHCP options which
        # belongs to this 'subnet_id' and frame the commands to update them.
        port_dhcp_options = []
        for row in self._tables['DHCP_Options'].rows.values():
            external_ids = getattr(row, 'external_ids', {})
            port_id = external_ids.get('port_id')
            if subnet_id == external_ids.get('subnet_id'):
                if port_id:
                    port_dhcp_options.append({'port_id': port_id,
                                             'port_dhcp_opts': row.options})

        for port_dhcp_opt in port_dhcp_options:
            if columns.get('options'):
                updated_opts = dict(columns['options'])
                updated_opts.update(port_dhcp_opt['port_dhcp_opts'])
            else:
                updated_opts = {}
            commands.append(
                self.add_dhcp_options(subnet_id,
                                      port_id=port_dhcp_opt['port_id'],
                                      options=updated_opts))

        return commands

    def get_address_sets(self):
        address_sets = {}
        for row in self._tables['Address_Set'].rows.values():
            if ovn_const.OVN_SG_NAME_EXT_ID_KEY not in (row.external_ids):
                continue
            name = getattr(row, 'name')
            data = {}
            for row_key in six.iterkeys(getattr(row, "_data", {})):
                data[row_key] = getattr(row, row_key)
            address_sets[name] = data
        return address_sets


class OvsdbSbOvnIdl(ovn_api.SbAPI):

    ovsdb_connection = None

    def __init__(self, driver, trigger=None):
        super(OvsdbSbOvnIdl, self).__init__()
        try:
            if OvsdbSbOvnIdl.ovsdb_connection is None:
                OvsdbSbOvnIdl.ovsdb_connection = get_connection(OvsdbSbOvnIdl,
                                                                trigger)
            if isinstance(OvsdbSbOvnIdl.ovsdb_connection,
                          ovsdb_monitor.OvnConnection):
                # We only need to know the content of Chassis in OVN_Southbound
                OvsdbSbOvnIdl.ovsdb_connection.start(
                    driver, table_name_list=['Chassis'])
            else:
                OvsdbSbOvnIdl.ovsdb_connection.start()
            self.idl = OvsdbSbOvnIdl.ovsdb_connection.idl
            self.ovsdb_timeout = cfg.get_ovn_ovsdb_timeout()
        except Exception as e:
            connection_exception = OvsdbConnectionUnavailable(
                db_schema='OVN_Southbound', error=e)
            LOG.exception(connection_exception)
            raise connection_exception

    def get_chassis_hostname_and_physnets(self):
        chassis_info_dict = {}
        for ch in self.idl.tables['Chassis'].rows.values():
            bridge_mappings = ch.external_ids.get('ovn-bridge-mappings', '')
            mapping_dict = n_utils.parse_mappings(bridge_mappings.split(','))
            chassis_info_dict[ch.hostname] = mapping_dict.keys()
        return chassis_info_dict

    def get_all_chassis(self, chassis_type=None):
        # TODO(azbiswas): Use chassis_type as input once the compute type
        # preference patch (as part of external ids) merges.
        chassis_list = []
        for ch in self.idl.tables['Chassis'].rows.values():
            if not chassis_type:
                chassis_list.append(ch.name)
            else:
                node_types = ch.external_ids.get('ovn-chassis-types', '')
                node_tuples = node_types.split(',')
                if chassis_type in node_tuples:
                    chassis_list.append(ch.name)
        return chassis_list
