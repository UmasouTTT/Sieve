import copy
import math

from bitarray import bitarray

class Node:
    def __init__(self, previous_node, next_node, is_leaf, parent=None, branching_factor=16):
        self.previous = previous_node
        self.next = next_node
        self.parent = parent
        self.branching_factor = branching_factor
        self.keys = []  # NOTE: must keep keys sorted
        self.children = []  # NOTE: children must correspond to parents.
        self.is_leaf = is_leaf

    def set_children(self, keys, children):
        self.keys = keys
        self.children = children
        if not self.is_leaf:
            for child in children:
                child.parent = self

    def split(self):
        is_leaf = False
        if self.is_leaf:
            new_node_keys = self.keys[(len(self.keys) // 2):]
            new_node_children = self.children[(len(self.children) // 2):]
            self.keys = self.keys[:(len(self.keys) // 2)]
            self.children = self.children[:(len(self.children) // 2)]
            is_leaf = True
            k = new_node_keys[0]
        else:
            new_node_keys = self.keys[((len(self.keys) + 1) // 2) - 1:]
            new_node_children = self.children[(len(self.children) // 2):]
            self.keys = self.keys[:((len(self.keys) + 1) // 2) - 1]
            self.children = self.children[:(len(self.children) // 2)]
            k = new_node_keys.pop(0)
        new_node = Node(self, self.next, is_leaf, self.parent, self.branching_factor)
        new_node.set_children(new_node_keys, new_node_children)
        self.next = new_node

        return new_node, k


class Segment:
    def __init__(self, slope, start, end, keys, blocks, row_group_num):
        self.start_key = start  # (key, location) tuple
        self.end_key = end
        self.slope = slope
        self.all_blocks = bitarray()
        self.all_blocks.extend([0] * row_group_num)

        # sorted
        self.indirection_keys = copy.deepcopy(keys)
        # compress
        self.indirection_blocks = blocks

        # sorted
        self.buffer = []
        self.buffer_indirection_blocks = []

    def _blockset_2_bitarray(self, blocksets, row_group_num):
        self.indirection_blocks = []
        for blockset in blocksets:
            row_groups_set = bitarray()
            row_groups_set.extend([0] * row_group_num)
            for block in blockset:
                row_groups_set[block] = 1
                self.all_blocks[block] = 1
            self.indirection_blocks.append(row_groups_set)

    def all_blocks_idx(self):
        return self.all_blocks

    def obtain_blocks(self, idx, file_name):
        if idx == -1:
            return set()
        if idx > len(self.indirection_keys) + len(self.buffer) - 1:
            print("Wrong data length for segment ...")
            return False
        if idx >= len(self.indirection_keys):
            return self._bitarray_to_blocks(self.buffer_indirection_blocks[idx - len(self.indirection_keys)], file_name)
        else:
            return self._bitarray_to_blocks(self.indirection_blocks[idx], file_name)

    def obtain_blocks_idx(self, idx):
        if idx > len(self.indirection_keys) + len(self.buffer) - 1:
            print("Wrong data length for segment ...")
            return False
        if idx >= len(self.indirection_keys):
            return self.buffer_indirection_blocks[idx - len(self.indirection_keys)]
        else:
            return self.indirection_blocks[idx]

    def _bitarray_to_blocks(self, blockset, file_name):
        res_set = set()
        for i in range(len(blockset)):
            if blockset[i] == 1:
                res_set.add(file_name + "-" + str(i))
        return res_set


    def add_block(self, idx, block):
        if idx > len(self.indirection_keys) + len(self.buffer) - 1:
            print("Wrong data length for segment ...")
            return False
        self.all_blocks[block] = 1
        if idx >= len(self.indirection_keys):
            self.buffer_indirection_blocks[idx - len(self.indirection_keys)][block] = 1
        else:
            self.indirection_blocks[idx][block] = 1

    def make_data(self):
        keys_block = {}
        for _ in self.indirection_keys:
            keys_block[_] = self.indirection_blocks[self.indirection_keys.index(_)]
        for _ in self.buffer:
            keys_block[_] = self.buffer_indirection_blocks[self.buffer.index(_)]
        # positions = [_ for _ in range(len(keys))]
        return keys_block

    def _array_2_blocks(self, block_array):
        block_set = set()
        for i in range(len(block_array)):
            if block_array[i] == 1:
                block_set.add(i)
        return block_set

    def add_buffer(self, block, row_group_num):
        row_groups_set = bitarray()
        row_groups_set.extend([0] * row_group_num)
        row_groups_set[block] = 1
        self.buffer_indirection_blocks.append(row_groups_set)

        self.all_blocks[block] = 1

    def linear_search(self, start, end, error):
        # Binary search segment and buffer and return value
        # keys
        blocks = bitarray()
        blocks.extend([0] * len(self.indirection_blocks[0]))
        start, end = self._search_range(start, end, error, self.indirection_keys)
        if start != -1 and end != -1:
            for _ in range(start, end+1):
                blocks |= self.indirection_blocks[_]
        # buffer
        # can be faster
        for _ in range(len(self.buffer)):
            if self.buffer[_] >= start and self.buffer[_] <= end:
                blocks |= self.buffer_indirection_blocks[_]

        return blocks

    def _search_range(self, start, end, error, keys):
        start_position = -1
        end_position = -1
        # start
        estimate_position = int((start - self.start_key) * self.slope)
        min_start = max(0, estimate_position - error)
        for _ in range(min_start, len(keys)):
            if keys[_] >= start:
                start_position = _
                break
        # end
        estimate_position = math.ceil((end - self.start_key) * self.slope)
        max_end = min(len(keys) - 1, estimate_position + error)
        for _ in range(max_end, 0, -1):
            if keys[_] <= end:
                end_position = _
                break
        return start_position, end_position