from copy import deepcopy
from redis_cm import Node, Nodes
from unittest.mock import patch


def cluster_nodes():
    return {'192.168.56.101:7002@17002': 
               {'node_id': 'f3bd1e6c707ddbbe2f6f74298e55173cda1f9427', 'flags': 'slave', 'master_id': '5814ec708ca5f0e8e042c54c382e4834186e78c0', 'last_ping_sent': '0', 'last_pong_rcvd': '1604380080000', 'epoch': '6', 'slots': [], 'connected': True}, 
            '192.168.56.102:7001@17001': 
                {'node_id': '3f6f88e6607b65327fa581ca9bccf6793cc9a66f', 'flags': 'myself,master', 'master_id': '-', 'last_ping_sent': '0', 'last_pong_rcvd': '1604380078000', 'epoch': '5', 'slots': [['0', '5460'], ['[5460', '>', '5814ec708ca5f0e8e042c54c382e4834186e78c0]']], 'connected': True},
            '192.168.56.101:7003@17003': 
                {'node_id': '9803f43a395969edc2cb6822b09d45eb3d0d27ed', 'flags': 'master', 'master_id': '-', 'last_ping_sent': '0', 'last_pong_rcvd': '1604380079000', 'epoch': '3', 'slots': [['10923', '16383']], 'connected': True}, 
            '192.168.56.103:7001@17001': 
                {'node_id': '266a0f11f0add8a10390567c1e72ed1eba87df8b', 'flags': 'slave', 'master_id': '3f6f88e6607b65327fa581ca9bccf6793cc9a66f', 'last_ping_sent': '0', 'last_pong_rcvd': '1604380081083', 'epoch': '5', 'slots': [], 'connected': True}, 
            '192.168.56.102:7003@17003': 
                {'node_id': '802fc68d0dfaac2e271c9c944d45d659daf6d2a8', 'flags': 'slave', 'master_id': '9803f43a395969edc2cb6822b09d45eb3d0d27ed', 'last_ping_sent': '0', 'last_pong_rcvd': '1604380080080', 'epoch': '3', 'slots': [], 'connected': True}, 
            '192.168.56.103:7002@17002': 
                {'node_id': '5814ec708ca5f0e8e042c54c382e4834186e78c0', 'flags': 'master', 'master_id': '-', 'last_ping_sent': '0', 'last_pong_rcvd': '1604380079079', 'epoch': '6', 'slots': [['5461', '10922']], 'connected': True}}


def clear_myself_flag(nodes):
    return {
        addr: {**info, 
               'flags': ','.join(flag for flag in info['flags'].split(',') if 'myself' != flag)}
        for addr, info in nodes.items()
    }
    
    
def add_myself_to_flags(nodes, addr):
    new_nodes = deepcopy(nodes)
    mynode = new_nodes[addr]
    mynode['flags'] = f"myself,{mynode['flags']}"
    return new_nodes


def cluster_nodes_nodes():
    nodes_nodes = []
    default_cluster_nodes = clear_myself_flag(cluster_nodes())
    for addr, info in default_cluster_nodes.items():
        node = Node(addr)
        with patch.object(node, '_cluster_nodes',
            return_value=add_myself_to_flags(default_cluster_nodes, addr)):
            node.load_info()
            nodes_nodes.append(node) 
    return Nodes(nodes_nodes)

