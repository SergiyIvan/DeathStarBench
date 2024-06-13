#!/usr/bin/python

import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import json

RPS = [100, 200, 400, 600, 800, 1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000]

def load_latency(filename_prefix):
    latencies = []
    filenames = [filename_prefix + str(x) for x in RPS]
    for filename in filenames:
        with open(filename) as log_file:
            lines = log_file.readlines()
            print("For filename: " + filename + ": " + [x.split()[1] for x in lines if "Requests/sec:" in x][0])
            latency_str = [x.split()[1] for x in lines if "99.999" in x][0]
            latency = 0
            if "ms" in latency_str:
                latency = float(latency_str[:-2])  # milliseconds
            else:
                latency = float(latency_str[:-1]) * 1000  # seconds
            latencies.append(latency)
    return [RPS, latencies]


mono = load_latency("/tmp/benchmark_sn_res_mono/res_")
micr = load_latency("/tmp/benchmark_sn_res_micr/res_")

matplotlib.rcParams.update({'font.size': 16})
plt.rcParams["figure.figsize"] = (10, 4)
plt.plot(mono[0], mono[1], linestyle = "-", marker = "x", linewidth = 3, label = "Monolith")
plt.plot(micr[0], micr[1], linestyle = "--", marker = "x", linewidth = 3, label = "Microservices")
plt.xlabel("RPS")
plt.ylabel("Tail Latency (ms)")
# plt.yscale('log')
plt.grid()
# plt.xlim(xmin=0, xmax=1200)
plt.legend(ncol=1, loc='upper left')
plt.tight_layout()
# plt.savefig("latency-rps.pdf")
plt.savefig("latency-rps.png", dpi=300, bbox_inches="tight")
