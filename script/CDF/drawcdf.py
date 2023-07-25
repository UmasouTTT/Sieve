import copy
import sys
import matplotlib.pyplot as plt
import pyarrow.parquet as pp


def segmentData(data, segment_error):
    segment_error = segment_error
    segments = []
    blkchanges = []
    segment_sl = []
    sl_high = sys.maxsize
    sl_low = 0
    segment = []
    blkchange = []

    last_rgs = set()
    y = 0
    prekey = None
    sort_data_keys = sorted(data.keys())
    for key in sort_data_keys:
        if key in data.keys():
            belonged_rg = data[key]
        else:
            belonged_rg = [None]
        if 0 == len(segment):
            segment.append(key)
            last_rgs = belonged_rg
            y = 0
            blkchange.append(y)
        else:
            # is violate error limit?
            _y = y
            if prekey != None and key - prekey > 1:
                _y += 2
            else:
                for _ in belonged_rg:
                    if _ not in last_rgs:
                        _y += 1
                        break
            _sl = _y / (key - segment[0])
            if _sl > sl_high or _sl < sl_low or (segment_error <= 1 and (key - segment[-1]) > 1):  # 最后一个条件新加的应对gap
                segments.append(copy.deepcopy(segment))
                blkchanges.append(copy.deepcopy(blkchange))
                if len(segment) == 1:
                    segment_sl.append(1)
                else:
                    if y != 0:
                        segment_sl.append(y / (segment[-1] - segment[0] + 1))
                    else:
                        segment_sl.append(1 / (segment[-1] - segment[0] + 1))
                segment.clear()
                blkchange.clear()
                sl_high = sys.maxsize
                sl_low = 0
                segment.append(key)
                last_rgs = belonged_rg
                y = 0
                blkchange.append(y)
            else:
                _sl_high = (_y + segment_error) / (key - segment[0] + 1)
                _sl_low = (_y - segment_error) / (key - segment[0] + 1)
                sl_high = min(sl_high, _sl_high)
                sl_low = max(sl_low, _sl_low)
                segment.append(key)
                last_rgs = belonged_rg
                y = _y
                blkchange.append(y)
        prekey = key
    if len(segment) == 1:
        segment_sl.append(1)
    else:
        if y != 0:
            segment_sl.append(y / (segment[-1] - segment[0] + 1))
        else:
            segment_sl.append(1 / (segment[-1] - segment[0] + 1))
    segments.append(copy.deepcopy(segment))
    sort_data_keys.clear()
    blkchanges.append(copy.deepcopy(blkchange))
    return segments, segment_sl, blkchanges

def readdata(parquetpath, column):
    data = {}
    table = pp.ParquetFile(parquetpath)
    num_of_row_groups = table.num_row_groups
    for row_group_index in range(num_of_row_groups):
        row_group_contents = table.read_row_group(row_group_index, columns=[column])
        for record in row_group_contents.column(column):
            if str(record) == 'None':
                continue
            recordv = int(str(record))
            if recordv not in data:
                data[recordv] = set()
            data[recordv].add(row_group_index)
    return data

def drawpic(xdata, ydata):
    plt.plot(xdata, ydata, alpha=1, linewidth=1.5)
    plt.ylabel('block changes', fontsize=10 + 4, fontweight='bold')  # accuracy
    plt.xlabel('key', fontsize=10 + 4, fontweight='bold')  # accuracy
    plt.savefig("sparseworsecdf.pdf", bbox_inches='tight')

if __name__=="__main__":
    data = readdata("/mydata/bigdata/data/diff_density/100/100.parquet", "lon")
    print("read data done")
    errors = [1, 100, 10000]
    labels = ["Error 1", "Error 100", "Error 10000"]
    colors = ["#1f77b4", "#ff7f0e", "#2ca02c"]
    for errorindex in range(len(errors)):
        segments, segment_sl, blkchanges = segmentData(data, errors[errorindex])
        print("error {} donw".format(errors[errorindex]))
        for i in range(len(segments)):
            if i == 0:
                plt.plot(segments[i], blkchanges[i], alpha=1, linewidth=1, color=colors[errorindex],label=labels[errorindex])
            else:
                plt.plot(segments[i], blkchanges[i], alpha=1, linewidth=1, color=colors[errorindex])
    plt.legend(loc=(0.55, 0.7), ncol=1, borderpad=0.4, handletextpad=0.3, columnspacing=0.8, fontsize=12,
               borderaxespad=-0.2, frameon=True, handlelength=1.5)
    plt.ylabel('block changes', fontsize=10 + 4, fontweight='bold')  # accuracy
    plt.xlabel('key', fontsize=10 + 4, fontweight='bold')  # accuracy
    plt.tight_layout()
    plt.savefig("sparseworsecdf.pdf", bbox_inches='tight')