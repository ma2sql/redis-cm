import redis
from xprint import xprint

def summarize_slots(slots):
    _temp_slots = []
    for slot in sorted(slots):
        if not _temp_slots or _temp_slots[-1][-1] != (slot-1): 
            _temp_slots.append([])
        _temp_slots[-1][1:] = [slot]
    return ','.join(map(lambda slot_exp: '-'.join(map(str, slot_exp)), _temp_slots)) 


def parse_slots(slots):
    parsed_slots = []
    for s in slots:
        # ["0", "5460"]
        if len(s) == 2:
            start = int(s[0])
            end = int(s[1]) + 1
            parsed_slots += list(range(start, end))
        # ["5462"]
        else:
            parsed_slots += [int(s[0])]

    return parsed_slots

class ParseHelperError(Exception): pass
class ParseHelper:
    @classmethod
    def parse_addr(cls, addr):
        s = addr.split('@')[0].split(':')[:2]
        if len(s) < 2:
            raise ParseHelperError(f"Invalid IP or Port (given as {addr}) - use IP:Port format")
        host = ':'.join(s[:-1])
        port = int(s[-1])
        return host, port

    @classmethod
    def parse_slots(cls, slots):
        parsed_slots = []
        migrating = {}
        importing = {}
        for s in slots:
            s_len = len(s)
            if s_len == 1:
                parsed_slots += [int(s[0])]
            elif s_len == 2:
                start, end = map(int, s)
                parsed_slots += list(range(start, end+1))
            elif s_len == 3:
                slot, direction, node_id = s
                slot = int(slot[1:])
                node_id = node_id[:-1]
                if direction == '>':
                    migrating.update({slot: node_id})
                else:
                    importing.update({slot: node_id})

        return sorted(parsed_slots), migrating, importing        

    @classmethod
    def parse_flags(cls, flags):
        return flags.split(',')
 

class NodeException(Exception): pass
class Node:
    def __init__(self, addr, password=None):
        self._origin_addr = addr
        self._password = password
        self._friends = []
        self._host, self._port = ParseHelper.parse_addr(addr)
        self._r = None
        self._info = {}

    def connect(self):
        if self._r:
            return
        xprint.verbose(f"Connecting to node {self}: ", end="")
        try:
            self._r = redis.StrictRedis(self._host, self._port,
                                        password=self._password,
                                        socket_timeout=3, decode_responses=True)
            self._r.ping()
        except redis.exceptions.RedisError as e:
            xprint.verbose("FAIL", ignore_header=True)
            self._r = None
            raise NodeException(f"Sorry, can't connect to node '{self}'. Reason: {e}")
        xprint.verbose("OK", ignore_header=True)

    def _cluster_nodes(self):
        return self._r.cluster('NODES')

    def load_info(self, with_friends=False):
        self._friends = []
        nodes = self._cluster_nodes() 

        for addr, info in nodes.items():
            host, port = ParseHelper.parse_addr(addr)
            flags = ParseHelper.parse_flags(info['flags'])
            if 'myself' in flags:
                slots, migrating, importing = ParseHelper.parse_slots(info['slots'])
                replicate = info['master_id'] if info['master_id'] != '-' else None
                self._info = {
                    'node_id': info['node_id'],
                    'host': host,
                    'port': port,
                    'flags': flags,
                    'slots': slots,
                    'migrating': migrating,
                    'importing': importing,
                    'replicate': replicate,
                }
            elif with_friends:
                self._friends.append({f"{host}:{port}": flags})

    @property
    def friends(self):
        return self._friends

    def __str__(self):
        return f"{self._host}:{self._port}" 

    @property
    def host(self):
        return self._info.get('host')

    @property
    def port(self):
        return self._info.get('port')

    @property
    def node_id(self):
        return self._info.get('node_id')

    @property
    def migrating(self):
        return self._info.get('migrating') or {}

    @property
    def importing(self):
        return self._info.get('importing') or {}

    @property
    def friends(self):
        return self._friends 

    @property
    def slots(self):
        return self._info.get('slots')

    @property
    def flags(self):
        return self._info.get('flags') 

    def is_master(self):
        return 'master' in self._info.get('flags')

    def is_slave(self):
        return 'slave' in self._info.get('flags')
   
    def config_signature(self):
        signature = []
        for n in [self._node] + self._friends:
            signature.append(f"{n['node_id']}:{summarize_slots(parse_slots(n['slots']))}")
        return '|'.join(sorted(signature))
            
    def cluster_count_keys_in_slot(self, slot):
        '''
        CLUSTER COUNTKEYSINSLOT __slot__
        '''
        return 0

    def clear_slot(self, slot):
        # CLUSTER SETSLOT __slot__ STABLE
        pass

    def set_slot_owner(self, slot):
        '''
        self._r.pipeline()\
            .cluster('DELSLOTS', slot)\
            .cluster('ADDSLOTS', slot)\
            .cluster('BUMPEPOCH')\
            .execute() 
        '''

    def cluster_bumpepoch(self):
        '''
        CLUSTER BUMPEPOCH
        '''

    def cluster_setslot_stable(self, slot):
        pass

    def cluster_setslot_node(self, slot, owner):
        self._r.cluster('SETSLOT', slot, 'NODE', owner.node_id)

    def cluster_setslot_migrating(self, slot, target):
        self._r.cluster('SETSLOT', slot, 'MIGRATING', target.node_id)
 
    def cluster_setslot_importing(self, slot, source):
        self._r.cluster('SETSLOT', slot, 'IMPORTING', source.node_id)

    def cluster_delslots(self, slots):
        pass

    def cluster_get_keys_in_slot(self, slot, count):
        return self._r.cluster('GETKEYSINSLOT', slot, count)

    def migrate(self, dst, keys_in_slot, password, timeout=60, replace=False): 
        # migrate(self, host, port, keys, destination_db, timeout, copy=False, replace=False, auth=None)
        self._r.migrate(dst.host, dst.port, keys_in_slot, 0, timeout,
                        auth=password, replace=replace)
   


class Nodes:
    def __init__(self, nodes, password=None):
        self._nodes = nodes
        self._password = password

    def __getitem__(self, key):
        return self._nodes[key]

    @property
    def covered_slots(self):
        return set(slot for n in self
                        for slot in n.slots)

    @property
    def open_slots(self):
        total_open_slots = set()
        for node in self:
            for open_type in [MIGRATING, IMPORTING]:
                slots = getattr(node, open_type) 
                if slots:
                    total_open_slots = total_open_slots.union(set(slots.keys()))
        return sorted(total_open_slots)


    def __iter__(self):
        for node in self._nodes:
            yield node

    def __eq__(self, _nodes):
        return self._nodes == _nodes

    @property
    def masters(self):
        for node in self._nodes:
            if node.is_master():
                yield node


IMPORTING = 'importing'
MIGRATING = 'migrating'


class CheckCluster:

    def __init__(self, nodes=None):
        self._nodes = nodes

    def check(self, quiet=False):
        if not quiet:
            self._show_nodes()
        self._check_config_consistency()
        self._check_open_slots()
        self._check_slots_coverage()

    def _check_config_consistency(self):
        if not self._is_config_consistent():
            self._increase_num_errors()
            xprint.error("Nodes don't agree about configuration!")
        else:
            xprint.ok("All nodes agree about slots configuration.")

    def check_open_slots(self):
        opened_slots = set()
        for node in self._nodes:
            for open_type in [MIGRATING, IMPORTING]:
                slots = getattr(node, open_type) 
                if slots:
                    xprint.warning(self._warn_opened_slot(node, open_type, slots.keys()))
                    opened_slots = opened_slots.union(set(slots.keys()))
        return opened_slots

    def _check_open_slots(self):
        xprint(">>> Check for open slots...")
        open_slots = set()
        for n, migrating, importing in self._get_opened_slots():
            if len(migrating) > 0:
                self._increase_num_errors()
                xprint.warning(self._warn_opened_slot(n, 'migrating', migrating.keys()))
                open_slots = open_slots.union(set(migrating.keys()))
            if len(importing) > 0:
                self._increase_num_errors()
                xprint.warning(self._warn_opened_slot(n, 'importing', importing.keys()))
                open_slots = open_slots.union(set(importing.keys()))

        if len(open_slots) > 0:
            xprint.warning(f"The following slots are open: "\
                           f"{','.join(map(str, open_slots))}")

        return open_slots

    def check_slots_coverage(self):
        return list(set(range(CLUSTER_HASH_SLOTS)) - self._nodes.covered_slots)

    def _check_slots_coverage(self):
        xprint(">>> Check slots coverage...")
        covered_slots = self._get_covered_slots()
        if len(covered_slots) == CLUSTER_HASH_SLOTS:
            xprint.ok(f"All {CLUSTER_HASH_SLOTS} slots covered.")
        else:
            self._increase_num_errors()
            xprint.error(f"Not all {CLUSTER_HASH_SLOTS} {summarize_slots(covered_slots)} "
                         f"slots are covered by nodes.")

        return list(set(range(CLUSTER_HASH_SLOTS)) - covered_slots)

    def _warn_opened_slot(self, node, open_type, slots):
        return f"Node {node} has slots in {open_type} "\
               f"state {','.join(map(str, slots))}"

    def is_config_consistent(self):
        return len(set(n.config_signature() for n in self._nodes)) == 1

class MoveSlot:
    def __init__(self, src, dst, slot):
        self._src = src
        self._dst = dst
        self._slot = slot

    def move_slot(self):
        return self

    def moving(self):
        return self

    def update(self):
        return self

    def notify(self):
        return self


class FixCluster:
    def __init__(self, nodes):
        self._nodes = nodes


class FixOpenSlot:
    def __init__(self, nodes, slot):
        self._nodes = nodes
        self._owner = None
        self._candidates = []
        self._migrating = []
        self._importing = []

    @property
    def nodes(self):
        return self._nodes

    def get_node_with_most_keys_in_slot(self, nodes, slot):
        best = None
        best_numkeys = 0

        for n in nodes:
            numkeys = n.cluster_count_keys_in_slot(slot)
            if numkeys > best_numkeys or best is None:
                numkeys = n.cluster_count_keys_in_slot(slot)
                best = n
                best_numkeys = numkeys

        return best

    def assert_owner(self):
        if self._owner is None:
            raise BaseException()

    def fix_open_slot(self, slot):
        self.set_owner_candidates(slot)
        owner = self.elect_owner()
        self.change_owner_to_donor()

        if (len(self._migrating) == 1
            and len(self._importing) == 1):
            src = self._migrating[0]
            dst = self._importing[0]
            move_slot(src, dst, slot, update=True)
        elif (len(self._migrating) == 0
              and len(self._importing) > 0):
            for n in self._importing:
                if n == owner:
                    continue
                move_slot(n, owner, slot, cold=True)
                n.clear_slot(slot)
                m = MoveSlot(n, owner, slot)
                m.move_slot()
        elif (len(self._migrating) == 1
              and len(self._importing) > 1):
            src = self._migrating[0]
            dst = None
            target_id = src.migrating['id']
            for n in self._importing:
                if n.cluster_count_keys_in_slot(slot) > 0:
                    raise BaseException()
                if target_id == n.node_id:
                    dst = n
            if dst:
                move_slot(src, dst, slot)
                for n in self._importing:
                    if n == dst:
                        continue
                    n.clear_slot(slot)
            else:
                src.clear_slot(slot)
                for n in self._importing:
                    n.clear_slot(n)


    def set_owner_candidates(self):
        self._candidates = []
        for n in self._nodes.masters:
            if slot in n.slots:
                self._candidates.append(n)
            elif n.cluster_count_keys_in_slot(self._slot):
                self._candidates.append(n)


    def elect_owner_from_candidates(self):
        owner = self.get_node_with_most_keys_in_slot(self._nodes, slot)
        owner.clear_slot(slot)
        owner.set_slot_owner(slot)
        owner.add_slots(slot)
        owner.cluster_bumpepoch()
        self._importing = [n for n in self._importing if n != owner]
        self._migrating = [n for n in self._migrating if n != owner]


    def change_owner_to_donor(self):
        if self._owner is None:
            raise BaseException('No owner')
        for n in self._owners:
            if n == self._owner:
                continue
            n.del_slots(slot)
            n.cluster_setslot_stable(slot)
            n.cluster_setslot_importing(slot, owner)
            self._importing = [_n for _n in self._importing if _n != n] 
            self._importing.append(n)
            self._migrating = [_n for _n in self._migrating if _n != n]


    def moving_slots(self, slot, owner):
        self._migrating = []
        self._importing = []

        for n in self._nodes.masters:
            if n.migrating.get(slot):
                self._migrating.append(n)
            elif n.importing.get(slot):
                self._importing.append(n)
            elif n != owner and n.cluster_count_keys_in_slot(slot) > 0:
                self._importing.append(n)


    def fix_open_slot_strategy(self, owners):
        # owners 구하기
        # owner is None ?
        # 1) owners가 0일 때,
        # 2) owners가 2 이상일 때
        # 즉, owners가 1이 아닐 때
        if not owners:
            return FixOpenSlotNoOwner
        return FixOpenSlotMultipleOwner


    def fix_on_case_1(self):
        src = self._migrating[0]
        dst = self._importing[0]
        move_slot(src, dst, slot, update=True)

    def fix_on_case_2(self):
        pass

    def fix_on_case_3(self):
        pass

    def fix_on_case_4(self):
        pass


class MoveSlot:
    def __init__(self, slot, src, dst, password=None):
        self._slot = slot
        self._src = src
        self._dst = dst
        self._password = password
        self._pipeline = 10
        self._timeout = 60
        self._fix = False
        self._dots = True

    def set_moving(self):
        self._dst.cluster_setslot_importing(self._slot, self._src) 
        self._src.cluster_setslot_migrating(self._slot, self._dst) 
        return self

    def update_config(self):
        self._src.del_slots(self._slot)
        self._dst.add_slots(self._slot)
        return self

    def notify(self, nodes):
        self._dst.cluster_setslot_node(self._slot, self._dst)
        self._src.cluster_setslot_node(self._slot, self._dst)
        for n in [n for n in nodes.masters
                    if n not in (self._src, self._dst)]:
            n.cluster_setslot_node(self._slot, self._dst)
        return self

    def move_slot(self):
        # only for 3.0.7++
        while keys_in_slot := self._src.cluster_get_keys_in_slot(self._slot, self._pipeline): 
            print(keys_in_slot)
            try:
                self._src.migrate(self._dst, keys_in_slot, self._password)
            except redis.exceptions.ResponseError as e:
                if self._fix and str(e).find('BUSYKEY'):
                    self._src.migrate(dst.host, dst.port, keys_in_slot,
                        timeout, auth=self._password, replace=True)
        return self

