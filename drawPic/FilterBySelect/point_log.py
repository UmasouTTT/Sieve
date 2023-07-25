import copy

import numpy as np
from matplotlib import pyplot as plt


plt.figure(figsize=(8, 3.8))

datasetnum = 3
index_num = 7
bar_width = 21
fontsize = 13

_y = [0.0001, 0.001, 0.01, 0.1, 1]
_ylabel = ["0.01", "0.1", "1", "10", "100"]

x_ticks = []
xs = [[] for i in range(index_num)]

for i in range(datasetnum):#四种选择度
    start = i * bar_width * (index_num + 1)
    tempx = [start + j * bar_width for j in range(index_num)]
    for j in range(len(tempx)):
        xs[j].append(tempx[j])
    if index_num % 2 == 1:
        x_ticks.append(tempx[int(index_num/2)])
    else:
        x_ticks.append((tempx[int(index_num/2)] + tempx[int(index_num/2)-1])/2)

x_names = ["Wikipedia(Sparse)", "Maps(Sparse)", "StoreSales(Dense)"]
totalblks = [48364, 80000, 5769]
indexfilterblks = [

]

#算sieve-0.1的假阳率
for i in range(3):
    print((indexfilterblks[3][i]-indexfilterblks[6][i])/indexfilterblks[3][i])

indexfilterblktexts = [

]

ys = []
for i in range(len(indexfilterblks)):
    tempy = []
    for j in range(len(indexfilterblks[i])):
        tempy.append(indexfilterblks[i][j] / totalblks[j])
    ys.append(copy.deepcopy(tempy))
labels =['ZoneMap', 'Fingerprint',  'Cuckoo(Bloom)', 'Sieve-0.1', 'Sieve-1', 'Sieve-10', 'FIT(Learned)']
colors = ["#696969", "#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#946cbd", "#8c564b"]
# 在每个柱子上方添加数字标签
for i in range(datasetnum):
    start = i * bar_width * (index_num + 1)
    tempx = [start + j * bar_width for j in range(index_num)]
    for j in range(len(tempx)):
        plt.text(tempx[j], ys[j][i] * 1.05, indexfilterblktexts[j][i], ha='center', rotation=0, fontsize=12, color=colors[j])

plt.yscale('log')
for i in range(index_num):
    plt.bar(xs[i], ys[i], width=bar_width, label=labels[i], color=colors[i])
handles, labels = plt.gca().get_legend_handles_labels()
plt.legend(handles, labels, bbox_to_anchor=(0.88, 1.035), loc='upper center', ncol=1, borderpad=0.4, handletextpad=0.3, columnspacing=1, fontsize=fontsize, borderaxespad=0.5, frameon=False, handlelength=1.5)
plt.minorticks_off()  # 去掉最小的刻度
# plt.title("ss_ticket_number")
plt.xticks(x_ticks, x_names, rotation=0, fontsize=fontsize)
plt.yticks(_y, _ylabel, fontsize=fontsize, rotation=0)
# plt.xlabel('Dataset', fontsize=fontsize+2)
plt.ylabel('Scan Ratio(%)', fontsize=fontsize+1, fontweight='bold', labelpad=0)
plt.tight_layout()
plt.savefig("point_query.pdf", bbox_inches='tight')
plt.show()