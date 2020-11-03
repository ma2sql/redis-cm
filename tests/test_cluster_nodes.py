import unittest
from unittest.mock import patch
import redis
from redis_cm import Nodes
from .fixture import cluster_nodes_nodes  

class testClusterNodes(unittest.TestCase):
    def setUp(self):
        self._cluster_nodes_nodes = cluster_nodes_nodes()

    def testCoveredSlots(self):
        self.assertSetEqual(self._cluster_nodes_nodes.covered_slots, set(range(16384)))

    def testOpenSlots(self):
        self.assertListEqual(self._cluster_nodes_nodes.open_slots, [5460])
        for slot in self._cluster_nodes_nodes.open_slots:
            for n in self._cluster_nodes_nodes:
                if n.importing.get(slot):
                    print(f'importing: {slot}')
                if n.migrating.get(slot):
                    print(f'migrating: {slot}')

    def tearDown(self):
        pass
