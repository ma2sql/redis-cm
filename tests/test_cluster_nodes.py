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

    def tearDown(self):
        pass
