# 画的是随着插入数据，各索引点查时间的变化
from matplotlib import pyplot as plt
import matplotlib

matplotlib.rcParams['pdf.fonttype'] = 42
matplotlib.rcParams['ps.fonttype'] = 42
plt.figure(figsize=(6, 3))

x_names = ["0.001%", "0.01%", "0.1%", "1%", "10%", "20%", "50%"]
x_axis_data = [1, 2, 3, 4, 5, 6, 7]
sieve_time = []
minmax_time = []
fingertime = []
fittime = []

for i in range(len(minmax_time)):
    minmax_time[i] *= 100
    fingertime[i] *= 100
    sieve_time[i] *= 100
    fittime[i] *= 100

plt.plot(x_axis_data, minmax_time, alpha=1, linewidth=1.5, label='ZoneMap', color="#696969" )
plt.plot(x_axis_data, fingertime, alpha=1, linewidth=1.5, label='Fingerprint', color="#1f77b4" )
plt.plot(x_axis_data, sieve_time, alpha=1, linewidth=1.5, label='Sieve', color="#2ca02c" )
plt.plot(x_axis_data, fittime, alpha=1, linewidth=1.5, label='FIT(Learned)', color="#8c564b" )
plt.legend(bbox_to_anchor=(0.85, 0.7), loc='upper center', ncol=1, borderpad=0.4, handletextpad=0.3, columnspacing=1, fontsize=12, borderaxespad=0.5, frameon=False, handlelength=1.5)
plt.xticks(x_axis_data, x_names, rotation=0, fontsize=13)
plt.ylabel("Index Search time (Sec)", fontsize=14, fontweight='bold')
plt.tight_layout()
plt.savefig("lookmap.pdf", bbox_inches='tight')
plt.show()