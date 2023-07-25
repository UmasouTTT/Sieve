class Rowgroupdict:
    def __init__(self):
        self.rglist = []#下标是编码值，值是完整路径
    def add_rowgroup(self, rowgroup):
        self.rglist.append(rowgroup)
    def get_rowgroup(self, rowgroupkey):
        return self.rglist[rowgroupkey]
    def get_rglist_len(self):
        return len(self.rglist)