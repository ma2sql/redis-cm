import unittest
from unittest.mock import patch
import redis
from redis_cm import Node, NodeException
from .fixture import cluster_nodes

class testClusterNode(unittest.TestCase):
    def setUp(self):
        self._cluster_nodes = cluster_nodes() 

    def testLoadInfo(self):
        with patch.object(Node, '_cluster_nodes', return_value=self._cluster_nodes):
            node = Node('192.168.56.102:7001')
            node.load_info(with_friends=False)
            self.assertDictEqual(
                {'node_id': '3f6f88e6607b65327fa581ca9bccf6793cc9a66f',
                 'host': '192.168.56.102',
                 'port': 7001,
                 'flags': ['myself', 'master'], 
                 'slots': list(range(0,5461)), 
                 'migrating': {5460: '5814ec708ca5f0e8e042c54c382e4834186e78c0'},
                 'importing': {},
                 'replicate': None},
                node._info)
            self.assertListEqual(node.friends, [])
 
    def testLoadInfoWithFriends(self):
        with patch.object(Node, '_cluster_nodes', return_value=self._cluster_nodes):
            node = Node('192.168.56.102:7001')
            node.load_info(with_friends=True)
            self.assertDictEqual(
                {'node_id': '3f6f88e6607b65327fa581ca9bccf6793cc9a66f',
                 'host': '192.168.56.102',
                 'port': 7001,
                 'flags': ['myself', 'master'], 
                 'slots': list(range(0,5461)), 
                 'migrating': {5460: '5814ec708ca5f0e8e042c54c382e4834186e78c0'},
                 'importing': {},
                 'replicate': None},
                node._info)
            self.assertListEqual(
                node.friends,
                [{'192.168.56.101:7002': ['slave']},
                 {'192.168.56.101:7003': ['master']},
                 {'192.168.56.103:7001': ['slave']},
                 {'192.168.56.102:7003': ['slave']},
                 {'192.168.56.103:7002': ['master']}])

    @unittest.mock.patch('redis.StrictRedis')
    def testConnect(self, mock_redis):
        r = unittest.mock.MagicMock()
        r.ping.return_value = None
        mock_redis.return_value = r
        node = Node('192.168.56.101:7001')
        node.connect()
        mock_redis.assert_called_with('192.168.56.101', 7001, password=None, socket_timeout=3, decode_responses=True)
        r.ping.assert_called()

    @unittest.mock.patch('redis.StrictRedis')
    def testConnect(self, mock_redis):
        r = unittest.mock.MagicMock()
        r.ping.side_effect = redis.exceptions.RedisError()
        mock_redis.return_value = r
        node = Node('192.168.56.101:7001')
        with self.assertRaises(NodeException):
            node.connect()

    def tearDown(self):
        pass
