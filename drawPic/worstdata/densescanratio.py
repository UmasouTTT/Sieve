import matplotlib.pyplot as plt
import numpy as np
# 非线性率和捕获率log的处理脚本
import matplotlib
matplotlib.rcParams['pdf.fonttype'] = 42
matplotlib.rcParams['ps.fonttype'] = 42
if __name__ == "__main__":
    # 画图相关
    plt.figure(figsize=(8, 4))
    x_axis_data = [10, 100, 1000, 10000]
    x_names = ["10", "100", "1000", "10000"]
    x_tickets = [10, 100, 1000, 10000]
    fsize = 12
    totalblk = 800

    y_sieve = []
    y_opt = []
    y_cuckoo = []
    y_finger = []
    y_minmax = []

    y_sieve = [(v * 100 /totalblk) for v in y_sieve]
    y_opt = [(v * 100 /totalblk) for v in y_opt]
    y_cuckoo = [(v * 100 /totalblk) for v in y_cuckoo]
    y_finger = [(v * 100 /totalblk) for v in y_finger]
    y_minmax = [(v * 100 /totalblk) for v in y_minmax]

    plt.plot(x_axis_data[0:5], y_minmax[0:5], alpha=1, linewidth=2, label='Zonemap', color="#696969")
    plt.plot(x_axis_data[0:5], y_finger[0:5], alpha=1, linewidth=2, label='Fingerprint', color="#1f77b4")
    plt.plot(x_axis_data[0:5], y_cuckoo[0:5], alpha=1, linewidth=2, label='Cuckoo(Bloom)', color="#ff7f0e")
    plt.plot(x_axis_data[0:5], y_sieve[0:5], alpha=1, linewidth=2, label='Sieve', color="#2ca02c")
    plt.plot(x_axis_data[0:5], y_opt[0:5], alpha=1, linewidth=2, label='FIT(Learned)', color="#8c564b")


    plt.xscale('log')
    plt.yscale('log')
    plt.legend(loc=(0.7, 0.23), ncol=1, borderpad=0.4, handletextpad=0.3, columnspacing=0.8, fontsize=fsize+2,
               borderaxespad=-0.2, frameon=False, handlelength=1.5)
    plt.xlabel('Error', fontsize=fsize + 4, fontweight='bold')

    plt.xticks(x_tickets[0:5], x_names[0:5], fontsize=fsize+3)
    plt.yticks([0.01, 0.1, 1, 10, 100], ['0.01', '0.1', '1', '10', '100'], fontsize=fsize+3)
    plt.grid(linestyle='--')  # 设置网格线段
    plt.minorticks_off()  # 去掉最小的刻度
    plt.ylabel('Scan Ratio(%)', fontsize=16, fontweight='bold')  # accuracy
    plt.xlabel('Error', fontsize=16, fontweight='bold')  # accuracy
    plt.tight_layout()
    plt.savefig("denseaccess.pdf", bbox_inches='tight')
    plt.show()