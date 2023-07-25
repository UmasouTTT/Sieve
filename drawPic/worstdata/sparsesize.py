import matplotlib.pyplot as plt
import numpy as np
# 非线性率和捕获率log的处理脚本
import matplotlib
matplotlib.rcParams['pdf.fonttype'] = 42
matplotlib.rcParams['ps.fonttype'] = 42
if __name__ == "__main__":
    # 画图相关
    plt.figure(figsize=(8, 4))

    x_axis_data = [1, 10, 100, 1000, 10000, 100000, 1000000]
    x_names = ["1", "10", "100", "1000", "10000", "100000", "1000000"]
    x_tickets = [1, 10, 100, 1000, 10000, 100000, 1000000]
    fsize = 12
    y_sieve = [0, 0, 0, 0, 0]
    y_opt = [0] * len(x_axis_data)
    y_cuckoo = [0] * len(x_axis_data)
    y_finger = [0] * len(x_axis_data)
    y_minmax = [0] * len(x_axis_data)

    plt.plot(x_axis_data[0:5], y_minmax, alpha=1, linewidth=2, label='Zonemap', color="#696969")
    plt.plot(x_axis_data[0:5], y_finger, alpha=1, linewidth=2, label='Fingerprint', color="#1f77b4")
    plt.plot(x_axis_data[0:5], y_cuckoo, alpha=1, linewidth=2, label='Cuckoo(Bloom)', color="#ff7f0e")
    plt.plot(x_axis_data[0:5], y_sieve, alpha=1, linewidth=2, label='Sieve', color="#2ca02c")
    plt.plot(x_axis_data[0:5], y_opt, alpha=1, linewidth=2, label='FIT(Learned)', color="#8c564b")


    plt.xscale('log')
    plt.yscale('log')
    plt.legend(loc=(0.7, 0.25), ncol=1, borderpad=0.4, handletextpad=0.3, columnspacing=0.8, fontsize=fsize+2,
               borderaxespad=-0.2, frameon=False, handlelength=1.5)
    plt.xlabel('Error', fontsize=fsize + 4, fontweight='bold')

    plt.xticks(x_tickets[0:5], x_names[0:5], fontsize=fsize+3)
    plt.yticks(fontsize=fsize+3)
    plt.grid(linestyle='--')  # 设置网格线段
    plt.minorticks_off()  # 去掉最小的刻度
    plt.ylabel('Index Size(B)', fontsize=16, fontweight='bold')  # accuracy
    plt.xlabel('Error', fontsize=16, fontweight='bold')  # accuracy
    plt.tight_layout()
    plt.savefig("sparsesize.pdf", bbox_inches='tight')
    plt.show()