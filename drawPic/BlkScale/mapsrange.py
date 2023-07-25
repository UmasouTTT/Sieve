import matplotlib.pyplot as plt
import numpy as np
# 非线性率和捕获率log的处理脚本
import matplotlib
matplotlib.rcParams['pdf.fonttype'] = 42
matplotlib.rcParams['ps.fonttype'] = 42
if __name__ == "__main__":
    # 画图相关
    plt.figure(figsize=(8, 4))
    x_axis_data = [1e4, 5e4, 1e5, 2e5]
    x_names = ["10000", "50000", "100000", "150000", "200000"]
    x_tickets = [10000, 50000, 100000, 150000, 200000]
    fsize = 12
    y_select1_sieve = []
    y_select1_opt = []
    y_select1_cuckoo = []
    y_select1_finger = []
    y_select1_minmax = []


    total_blks = [4000, 800, 400, 200]

    ys = [[] for i in range(5)]
    for i in range(len(y_select1_sieve)):
        ys[0].append(100*(y_select1_minmax[i])/ (total_blks[i]))
        ys[1].append(100*(y_select1_finger[i])/ (total_blks[i]))
        ys[2].append(100*(y_select1_cuckoo[i])/ (total_blks[i]))
        ys[3].append(100*(y_select1_sieve[i])/ (total_blks[i]))
        ys[4].append(100*y_select1_opt[i]/ total_blks[i])


    plt.plot(x_axis_data, ys[0], alpha=1, linewidth=2, label='ZoneMap', color="#696969")
    plt.plot(x_axis_data, ys[1], alpha=1, linewidth=2, label='Fingerprint', color="#1f77b4")
    plt.plot(x_axis_data, ys[2], alpha=1, linewidth=2, label='Cuckoo(Bloom)', color="#ff7f0e")
    plt.plot(x_axis_data, ys[3], alpha=1, linewidth=2, label='Sieve-0.1', color="#2ca02c")
    plt.plot(x_axis_data, ys[4], alpha=1, linewidth=2, label='FIT(Learned)', color="#8c564b")
    print(ys[3][-1] - ys[3][0])
    print(ys[3][1]/ys[3][0])

    # plt.xscale('log')
    plt.legend(loc=(0.62, 0.4), ncol=1, borderpad=0.4, handletextpad=0.3, columnspacing=0.8, fontsize=fsize+2,
               borderaxespad=-0.2, frameon=False, handlelength=1.5)
    plt.xlabel('Error', fontsize=fsize + 4, fontweight='bold')

    plt.xticks(x_tickets, x_names, fontsize=fsize+3)
    plt.yticks(fontsize=fsize+3)
    plt.grid(linestyle='--')  # 设置网格线段
    plt.minorticks_off()  # 去掉最小的刻度
    plt.ylabel('Scan Ratio(%)', fontsize=fsize + 4, fontweight='bold')  # accuracy
    plt.xlabel('Records Num Per Block', fontsize=fsize + 4, fontweight='bold')  # accuracy
    plt.savefig("maprange.pdf", bbox_inches='tight')
    plt.show()