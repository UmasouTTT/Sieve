import copy

import matplotlib.pyplot as plt
import numpy as np

# 生成数据
datasetnum = 2
index_num = 7
bar_width = 21
fontsize = 14

x_ticks1 = []
xs1 = [[] for i in range(index_num)]
for i in range(datasetnum):#四种选择度
    start = i * bar_width * (index_num + 1)
    tempx = [start + j * bar_width for j in range(index_num)]
    for j in range(len(tempx)):
        xs1[j].append(tempx[j])
    if index_num % 2 == 1:
        x_ticks1.append(tempx[int(index_num/2)])
    else:
        x_ticks1.append((tempx[int(index_num/2)] + tempx[int(index_num/2)-1])/2)

x_names = ["Wikipedia(Sparse)", "Maps(Sparse)", "StoreSales(Dense)"]
totalblks = [48364, 80000, 5769]
indexfilterblks = [

]

for i in range(3):
    print((indexfilterblks[3][i]-indexfilterblks[6][i])/indexfilterblks[3][i])

indexfilterblktexts = [

]
labels = ['ZoneMap', 'Fingerprint', 'Cuckoo(Bloom)', 'Sieve_0.1', 'Sieve_1', 'Sieve_10', 'FIT']
colors = ['#696969', "#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#946cbd", "#8c564b"]
ys = []
for i in range(len(indexfilterblks)):
    tempy = []
    for j in range(len(indexfilterblks[i])):
        tempy.append(indexfilterblks[i][j] / totalblks[j])
    ys.append(copy.deepcopy(tempy))
# 绘制两个子图
fig, axs = plt.subplots(1, 2, figsize=(8, 3.8), gridspec_kw={'width_ratios': [2, 1]})
fig.subplots_adjust(wspace=0)
# 绘制第一个子图
for i in range(index_num):
    axs[0].bar(xs1[i], ys[i][0:2], width=bar_width, color=colors[i])

for i in range(datasetnum):
    start = i * bar_width * (index_num + 1)
    tempx = [start + j * bar_width for j in range(index_num)]
    for j in range(len(tempx)):
        axs[0].text(tempx[j], ys[j][i] * 1.01, indexfilterblktexts[j][i], ha='center', rotation=0, fontsize=11, color=colors[j])


axs[0].set_xticks(x_ticks1)
axs[0].set_xticklabels(x_names[0:2], fontsize=fontsize)
axs[0].set_yticks([0, 0.2, 0.4, 0.6, 0.8, 1])
axs[0].set_yticklabels(["0", "20", "40", "60", "80", "100"], fontsize=fontsize)
# axs[0].tick_params(axis='y', labelrotation=45)

datasetnum = 1
x_ticks2 = []
xs2 = [[] for i in range(index_num)]
for i in range(datasetnum):#四种选择度
    start = i * bar_width * (index_num + 1)
    tempx = [start + j * bar_width for j in range(index_num)]
    for j in range(len(tempx)):
        xs2[j].append(tempx[j])
    if index_num % 2 == 1:
        x_ticks2.append(tempx[int(index_num/2)])
    else:
        x_ticks2.append((tempx[int(index_num/2)] + tempx[int(index_num/2)-1])/2)
# 绘制第二个子图
for i in range(index_num):
    axs[1].bar(xs2[i], ys[i][2:], width=bar_width, color=colors[i])
for i in range(datasetnum):
    start = i * bar_width * (index_num + 1)
    tempx = [start + j * bar_width for j in range(index_num)]
    for j in range(len(tempx)):
        axs[1].text(tempx[j], ys[j][i+2] * 1.01, indexfilterblktexts[j][i+2], ha='center', rotation=0, fontsize=11, color=colors[j])

axs[1].set_xticks(x_ticks2)
axs[1].set_xticklabels(x_names[2:], fontsize=fontsize)
axs[1].set_yscale('log')
axs[1].set_yticks([0.001, 0.01, 0.1])
axs[1].set_yticklabels(["0.1", "1", "10"], fontsize=fontsize)
# axs[1].tick_params(axis='y', labelrotation=45)
axs[1].minorticks_off()  # 去掉最小的刻度

# 设置图形级别的标签
# fig.text(0.5, 0, 'Dataset', ha='center', fontsize = fontsize+2)
# fig.text(0, 0.5, 'Scan Percentage', va='center', rotation='vertical', fontsize = 14)
# axs[1].legend(['FP(ZoneMap)', 'Cuckoo(Bloom)', 'Sieve-0.01', 'Sieve-0.1', 'Sieve-1', 'Fiting(learned)'], loc='center', bbox_to_anchor=(0.69, 0.83), frameon=False, fontsize=fontsize-2)

# 显示图形
plt.tight_layout()
plt.savefig("range0.001.pdf", bbox_inches='tight')

plt.show()