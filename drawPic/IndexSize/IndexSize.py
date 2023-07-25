import numpy as np
from matplotlib import pyplot as plt

plt.figure(figsize=(7.5, 3.5))

select_num = 3
index_num = 7
bar_width = 21
fontsize = 14

x_ticks = []
xs = [[] for i in range(index_num)]

for i in range(select_num):#四种选择度
    start = i * bar_width * (index_num + 1)
    tempx = [start + j * bar_width for j in range(index_num)]
    for j in range(len(tempx)):
        xs[j].append(tempx[j])
    if index_num % 2 == 1:
        x_ticks.append(tempx[int(index_num/2)])
    else:
        x_ticks.append((tempx[int(index_num/2)] + tempx[int(index_num/2)-1])/2)

x_names = ["Wikipedia(Sparse)", "Maps(Sparse)", "StoreSales(Dense)"]

indexsize = [

]# index-dataset-in MB

blknum = [48364, 80000, 5759]
dataSize = []
for i in range(3):
    dataSize.append(blknum[i] * 50000 * 64 / 8000000)#dataSize in MB

ys = [[] for i in range(len(indexsize))]
for i in range(len(indexsize)):
    for j in range(len(indexsize[i])):
        ys[i].append(indexsize[i][j]/dataSize[j])

labels = ['ZoneMap', 'Fingerprint',  'Cuckoo(Bloom)', 'Sieve-0.1', 'Sieve-1', 'Sieve-10', 'FIT(Learned)']
colors = ['#696969', "#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#946cbd", "#8c564b"]

plt.yscale('log')
for i in range(index_num):
    plt.bar(xs[i], ys[i], width = bar_width, label=labels[i], color=colors[i])

for i in range(select_num):
    start = i * bar_width * (index_num + 1)
    tempx = [start + j * bar_width for j in range(index_num)]
    for j in range(len(tempx)):
        if j == 0 and i >= 1:
            plt.text(tempx[j], ys[j][i] * 1.05, '{}\nMB'.format(indexsize[j][i]), ha='center', rotation=0, fontsize=8,
                     color=colors[j])
        else:
            plt.text(tempx[j], ys[j][i] * 1.05, '{}\nMB'.format(indexsize[j][i]), ha='center', rotation=0, fontsize=8, color=colors[j])

plt.minorticks_off()  # 去掉最小的刻度
plt.legend(bbox_to_anchor=(0.23, 1.02), loc='upper center', ncol=2, borderpad=0.4, handletextpad=0.3, columnspacing=0.5, fontsize=fontsize-3, borderaxespad=0.4, frameon=False, handlelength=1.3)
plt.ylim((0.00001, 29))
# plt.title("ss_ticket_number")
plt.xticks(x_ticks, x_names, rotation=0, fontsize=fontsize)
plt.yticks(fontsize=fontsize)
plt.yticks([0.00001, 0.0001, 0.001, 0.01, 0.1, 1], ["0.001", "0.01", "0.1", "1", "10", "100"], fontsize=fontsize)
plt.ylabel('IndexSize/ColumnSize(%)', fontsize=fontsize+2)
plt.tight_layout()
plt.savefig("indexSize.pdf", bbox_inches='tight')
plt.show()