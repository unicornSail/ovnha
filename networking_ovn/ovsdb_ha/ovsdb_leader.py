import threading
import etcd
from oslo_log import log
from networking_ovn.common import config

LOG = log.getLogger(__name__)


class OVSDBWatchLeaderThread(threading.Thread):
    def __init__(self, hosts):
        self.ovsDbNbOvnIdls = []

        threading.Thread.__init__(self)
        self.client = etcd.Client(hosts,
                                  allow_reconnect=True,
                                  allow_redirect=False,
                                  protocol='https',
                                  cert=(config.get_ovn_ovsdb_certificate_file(),
                                        config.get_ovn_ovsdb_private_key_file()),
                                  ca_cert=config.get_ovn_ovsdb_ca_cert_file(),)
        self.northDb_path = '/northdb'

    def run(self):
        for event in self.client.eternal_watch(self.northDb_path, recursive=True):
            leader_info = self.get_ovsdb_leader()
            LOG.info('ovsdb leader changed. new leader is %s. change info is %s.', leader_info, event)
            if leader_info is not None:
                for ovsDbNbOvnIdl in self.ovsDbNbOvnIdls:
                    ovsDbNbOvnIdl.__reconnect__(leader_info)

    def registered(self, ovsDbNbOvnIdl):
        if ovsDbNbOvnIdl not in self.ovsDbNbOvnIdls:
            self.ovsDbNbOvnIdls.append(ovsDbNbOvnIdl)

    def get_ovsdb_leader(self):
        directory = self.client.get(self.northDb_path)
        small_node = None
        for result in directory.children:
            if small_node is None:
                small_node = result
            if small_node.key > result.key:
                small_node = result
        if small_node is None:
            return None
        return small_node.value
