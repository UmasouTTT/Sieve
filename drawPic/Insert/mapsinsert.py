import math

import numpy as np
from matplotlib import pyplot as plt
import matplotlib
matplotlib.rcParams['pdf.fonttype'] = 42
matplotlib.rcParams['ps.fonttype'] = 42

insertnum = 7
indexnum = 4

plt.figure(figsize=(6, 3))
sizel = 12
n = 4
x_ticks = []
x0 = []
x1 = []
x2 = []
x3 = []
bar_width = 21
for i in range(insertnum):
    start = i * bar_width * (indexnum + 1)
    x = [start + j * bar_width for j in range(indexnum)]
    x_ticks.append((x[1]+x[2])/2)
    x0.append(x[0])
    x1.append(x[1])
    x2.append(x[2])
    x3.append(x[3])
x_names = ["0.001%", "0.01%", "0.1%", "1%", "10%", "20%", "50%"]

minmax_times = []
finger_times = []
sieve_times = []
fit_time = []

plt.yscale('log')

for i in range(len(minmax_times)):
    minmax_times[i] *= 100
    finger_times[i] *= 100
    sieve_times[i] *= 100
    fit_time[i] *= 100

plt.bar(x0, minmax_times, alpha=0.9, width = bar_width, label='ZoneMap', color='#696969')
plt.bar(x1, finger_times, alpha=0.9, width = bar_width, label='Fingerprint', color='#1f77b4')
plt.bar(x2, sieve_times, alpha=0.9, width = bar_width, label='Sieve', color='#2ca02c')
plt.bar(x3, fit_time, alpha=0.9, width = bar_width, label='FIT(Learned)', color='#8c564b')
plt.legend(loc='upper center', ncol=4, borderpad=0.4, handletextpad=0.3, columnspacing=0.7, fontsize=sizel, borderaxespad=0.1, frameon=False, handlelength=1)

# plt.title("wiki_pagecount", fontsize=sizel)
plt.ylim((0.01, 1e7))
plt.xticks(x_ticks, x_names, rotation=0, fontsize=sizel)
plt.yticks(fontsize=sizel)
plt.minorticks_off()
plt.ylabel("Insert time (Sec)", fontsize=sizel+2, fontweight='bold')
plt.tight_layout()
plt.savefig("insertmap.pdf", bbox_inches='tight')
plt.show()