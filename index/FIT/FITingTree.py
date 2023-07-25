import copy
from index.FIT.Node import *
import os
from bisect import bisect_left
import pyarrow.parquet as pp
import functools
from index.index import *
from index.util import *
from param import args


class FITtingTree:
    def __init__(self, error, buffer_error, file, column, branching_factor=16):
        self.key_type = 'int'
        self.branching_factor = branching_factor
        self.file_name = ""
        self.row_group_num = -1
        # self.root = Node(None, None, True, branching_factor=branching_factor)
        # seg = Segment(1, 0, 0)
        # self.root.set_children([0], [seg])
        self.error = error - buffer_error
        self.buffer_error = buffer_error
        if buffer_error > error:
            print('Buffer Error should be less than total error. Your values are\n'
                  'Buffer error: %f Total error: %f\n'
                  'Aborting...' % (self.buffer_error, self.error))
            exit()
        key_block_pairs = self._prepare(file, column)
        # for test
        # keys = [_ for _ in range(10)]
        # keys.extend([100+_ for _ in range(10)])
        # blocks = []
        # self.row_group_num = 20
        # for _ in range(20):
        #     tmp = set()
        #     tmp.add(_)
        #     blocks.append(copy.deepcopy(tmp))
        # locations = [_ for _ in range(len(keys))]
        # key_block_pairs = {}
        # for _ in keys:
        #     key_block_pairs[_] = blocks[keys.index(_)]
        self.segments = self.__shrinking_cone_segmentation(key_block_pairs)
        self.indexsize = len(self.segments) * 64 * 3 / 8 + len(key_block_pairs) * self.row_group_num / 8
        # for s in segments:
        #     self.__insert_segment(s, self.root)
        # self.segments.sort(key=functools.cmp_to_key(self.__compare_segment))


    def _prepare(self, file, column):
        datas = {}
        table = pp.ParquetFile(file)
        num_of_row_groups = table.num_row_groups
        self.file_name = file
        self.row_group_num = num_of_row_groups
        for row_group_index in range(num_of_row_groups):
            row_group_contents = table.read_row_group(row_group_index, columns=[column])
            for record in row_group_contents.column(column):
                record = getRecord(record)
                if str(record) == 'None':
                    continue
                if record not in datas:
                    datas[record] = bitarray(num_of_row_groups)
                    datas[record].setall(0)
                datas[record][row_group_index] = 1
        # locations = [_ for _ in range(len(keys))]
        return datas

    def __shrinking_cone_segmentation(self, key_block_pairs={}):
        # data is the keys and pointers, it must be sorted
        keys = sorted(key_block_pairs.keys())
        segments = []
        high_slope = float('inf')
        low_slope = 0
        origin_key = keys[0]
        origin_loc = 0
        loc = 0
        end_key = keys[0]
        # speed up
        tmp_keys = [keys[0]]
        tmp_blocks = [key_block_pairs[keys[0]]]
        for i in range(1, len(keys)):
            key = keys[i]
            loc += 1
            tmp_point_slope = (loc - origin_loc) / (key - origin_key)
            if low_slope <= tmp_point_slope <= high_slope:
                # Point is inside the cone
                tmp_high_slope = ((loc + self.error) - origin_loc) / (key - origin_key)
                tmp_low_slope = ((loc - self.error) - origin_loc) / (key - origin_key)
                high_slope = min(high_slope, tmp_high_slope)
                low_slope = max(low_slope, tmp_low_slope)
                end_key = key
                tmp_keys.append(key)
                tmp_blocks.append(key_block_pairs[key])
            else:
                #slope = (high_slope + low_slope) / 2
                if end_key == origin_key:
                    slope = 1
                else:
                    slope = ((loc - 1) - origin_loc) / (end_key - origin_key)

                new_segment = Segment(slope, origin_key, end_key, tmp_keys, tmp_blocks, self.row_group_num)
                high_slope = float('inf')
                low_slope = 0
                origin_key = key
                origin_loc = loc
                tmp_keys = [key]
                tmp_blocks = [key_block_pairs[key]]
                end_key = key
                segments.append(new_segment)

        # slope = (high_slope + low_slope) / 2
        if end_key == origin_key:
            slope = 1
        else:
            slope = ((loc - 1) - origin_loc) / (end_key - origin_key)


        new_segment = Segment(slope, origin_key, end_key, tmp_keys, tmp_blocks, self.row_group_num)
        segments.append(new_segment)

        return segments

    def __binary_segment_search(self, keys, key, start_pos=0, end_pos=None):
        pos = -1
        left = start_pos
        right = end_pos

        if left > len(keys) - 1:
            return -1
        if right < 0:
            return -1

        left = max(0, left)
        right = min(len(keys)-1, right)

        while left <= right:
            mid = int(left + (right - left) // 2)
            tmp_key = keys[mid]

            if tmp_key == key:
                pos = mid
                break
            elif tmp_key < key:
                left = mid + +1
            else:
                right = mid - 1

        return pos

    def __search_buffer(self, keys, key):
        pos = -1
        for k in keys:
            if k == key:
                pos = keys.index(k)
        return pos

    def __binary_search(self, segment, start_pos, end_pos, key):
        # Binary search segment and buffer and return value
        indirection_pos = self.__binary_segment_search(segment.indirection_keys, key, start_pos, end_pos)

        if indirection_pos != -1:
            return indirection_pos

        buffer_pos = self.__search_buffer(segment.buffer, key)

        if buffer_pos != -1:
            return len(segment.indirection_keys) + buffer_pos

        return -1



    def __search_segment(self, segment, key):
        position = (key - segment.start_key) * segment.slope
        return self.__binary_search(segment, max(0, position - self.error), position + self.error, key)

    def __search_tree(self, key):
        # find the segment that this key belongs to
        # current_node = self.root
        # while not current_node.is_leaf:
        #     idx = bisect_left(current_node.keys, key)
        #     if idx < len(current_node.keys) and current_node.keys[idx] == key:
        #         idx += 1
        #     current_node = current_node.children[idx]
        # A small amount of segments, remove tree structure to save more space for FITing
        #idx = self.__binary_search_list([_.start_key for _ in self.segments], key)

        left = 0
        right = len(self.segments)
        while left < right:
            mid = (left + right) // 2
            if self.segments[mid].start_key < key:
                left = mid + 1
            else:
                right = mid

        idx = left - 1

        if idx + 1 < len(self.segments) and self.segments[idx + 1] == key:
            idx += 1



        return self.segments[max(idx, 0)]


    def _search_segment_idx(self, key):
        seg = self.__search_tree(key)
        return seg

    def look_up(self, key):
        seg = self._search_segment_idx(key)
        return seg.obtain_blocks(self.__search_segment(seg, key), self.file_name)

    def __union_segment(self, segment, blocks, start, end):
        for _ in range(len(segment.indirection_keys)):
            if segment.indirection_keys[_] >= start and segment.indirection_keys[_] <= end:
                blocks = blocks.union(segment.obtain_blocks(segment.indirection_keys.index(_), self.file_name))
        for _ in segment.buffer:
            if _ >= start and _ <= end:
                blocks = blocks.union(segment.obtain_blocks(segment.buffer.index(_), self.file_name))
        return blocks

    def _union_block_idx(self, segment, blocks, start, end):
        for _ in range(len(segment.indirection_keys)):
            if segment.indirection_keys[_] >= start and segment.indirection_keys[_] <= end:
                blocks |= segment.indirection_blocks[_]
        for _ in segment.buffer:
            if _ >= start and _ <= end:
                blocks |= segment.buffer_indirection_blocks[_]
        return blocks

    def _union_entire_block(self, segment, blocks):
        for _ in range(len(segment.indirection_keys)):
            blocks |= segment.indirection_blocks[_]
        for _ in range(len(segment.buffer)):
            blocks |= segment.buffer_indirection_blocks[_]
        return blocks

    def range_search(self, start, end):
        blocks = bitarray()
        blocks.extend([0] * self.row_group_num)
        # can be faster
        start_seg = self._search_segment_idx(start)
        blocks = start_seg.linear_search(start, end, self.error)
        #blocks = self._union_block_idx(start_seg, blocks, start, end)
        end_seg = self._search_segment_idx(end)
        blocks |= end_seg.linear_search(start, end, self.error)
        #blocks = self._union_block_idx(end_seg, blocks, start, end)

        # ...
        start_seg_idx = self.segments.index(start_seg)
        end_seg_idx = self.segments.index(end_seg)
        if end_seg_idx > start_seg_idx + 1:
            for _ in range(start_seg_idx + 1, end_seg_idx):
                #blocks = self._union_entire_block(self.segments[_], blocks)
                blocks |= self.segments[_].all_blocks_idx()

        res_set = set()
        for i in range(len(blocks)):
            if blocks[i] == 1:
                res_set.add(self.file_name + "-" + str(i))
        return res_set




    def put(self, insert_key, block):
        seg = self._search_segment_idx(insert_key)
        indirection_pos = self.__search_segment(seg, insert_key)
        # if new block
        if block >= self.row_group_num:
            self._update_row_groups()
        if indirection_pos != -1:
            # exist
            seg.add_block(indirection_pos, block)
        else:
            # self.__update_indirection_layer(insert_key, [block])
            seg.buffer.append(insert_key)
            seg.add_buffer(block, self.row_group_num)
            if len(seg.buffer) > self.buffer_error:
                # regenerate
                keys_block_pair = seg.make_data()
                new_segments = self.__shrinking_cone_segmentation(keys_block_pair)
                #remove
                self.segments.remove(seg)
                # add
                for s in new_segments:
                    self.segments.append(s)
                self.segments.sort(key=functools.cmp_to_key(self.__compare_segment))

    def _update_row_groups(self):
        self.row_group_num += 1
        for seg in self.segments:
            seg.all_blocks.append(0)
            for _ in range(len(seg.indirection_blocks)):
                seg.indirection_blocks[_].append(0)
            for _ in range(len(seg.buffer_indirection_blocks)):
                seg.buffer_indirection_blocks[_].append(0)

    # def __insert_segment(self, segment, leaf):
    #     key = segment.start_key
    #     i = bisect_left(leaf.keys, key)
    #     leaf.keys.insert(i, key)
    #     leaf.children.insert(i, segment)
    #     node = leaf
    #     while len(node.children) >= self.branching_factor:
    #         new_child, k = node.split()  # node, new_child
    #         if node.parent is None:
    #             # create a parent node and break
    #             new_root = Node(None, None, False, None, self.branching_factor)
    #             new_root.set_children([k], [node, new_child])
    #             self.root = new_root
    #             break
    #         else:
    #             node = node.parent
    #             # add new node to parent
    #             i = bisect_left(node.keys, k)
    #             node.keys.insert(i, k)
    #             node.children.insert(i + 1, new_child)

    def __compare_segment(self, seg1, seg2):
        if seg1.start_key < seg2.start_key:
            return True
        return False


    @staticmethod
    def __binary_search_list(list_name, key):
        i = bisect_left(list_name, key) - 1
        if i + 1 < len(list_name) and list_name[i + 1] == key:
            i += 1
        return max(i, 0)

class FIT(Index):
    def __init__(self, directory, columns, error=args.segment_error, buffer_error=int(args.segment_error/2)):
        super(FIT, self).__init__(directory, columns)
        self.directory = directory
        self.columns = columns
        self.error = error
        self.buffer_error = buffer_error
        self.fits = {}

    def generateIndexes(self):
        files = os.listdir(self.directory)
        files = [self.directory + _ for _ in files]
        for column in self.columns:
            indexsize = 0
            self.fits[column] = {}
            for file in files:
                self.fits[column][file] = FITtingTree(self.error, self.buffer_error, file, column)
                indexsize += self.fits[column][file].indexsize
            print("FIT size is {}B".format(indexsize))

    def genPerFile(self, file, column):
        curFIT = FITtingTree(self.error, self.buffer_error, file, column)
        self.fits[column][file] = curFIT

    def range_search(self, data_range, column):
        searched_blocks = list()
        for file in self.fits[column]:
            if data_range[0] == data_range[1]:
                searched_blocks.extend(self.fits[column][file].look_up(data_range[0]))
            else:
                searched_blocks.extend(self.fits[column][file].range_search(data_range[0], data_range[1]))
        return searched_blocks

    def point_search(self, value, column):
        searched_blocks = list()
        for file in self.fits[column]:
            searched_blocks.extend(self.fits[column][file].look_up(value))
        return searched_blocks

    def insert(self, data, rg_id, column, file):
        self.fits[column][file].put(data, rg_id)



# F = FITtingTree(10, 3, "./data", ["col"])
# a = F.look_up(5)
# a = F.range_search(5, 7)
# F.put(10000, 20)
# F.put(10001, 21)
# F.put(10002, 22)
# F.put(10003, 23)
# print(a)
