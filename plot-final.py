#!/usr/bin/python

import sys
import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import json
from scipy import stats


CORRECTED_LATENCY = 0
UNCORRECTED_LATENCY = 1

ITERATIONS = 5
NCONNS = "2"
PERCENTILE = "99.000"
# RPS_WRITE = [200, 205, 210, 215, 220, 225, 230, 235, 240, 245, 250, 255, 260, 265, 270, 275, 280, 285, 290]
# RPS_WRITE = [25, 50, 75, 100, 110, 120, 130, 140, 150, 160, 170, 180, 190, 200, 205, 210, 215, 220, 225, 230, 235, 240, 245, 250, 255, 260, 265, 270, 275, 280, 285, 290]
RPS_WRITE = [1, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 110, 120, 130, 140, 150, 160, 170, 180, 190, 200, 205, 210, 215, 220, 225, 230, 235, 240, 245, 250, 255, 260, 265, 270, 275, 280, 285, 290]
# RPS_READ = [300, 325, 350, 360, 380, 400, 420, 440, 460, 480, 500, 520, 540, 560, 600]
RPS_READ = [1, 25, 50, 75, 100, 125, 150, 175, 200, 225, 250, 275, 300, 325, 350, 360, 380, 400, 420, 440, 460, 480, 500, 520, 540, 560, 600]


def load_latency(filename_prefix, RPS):
    mean_latencies_corrected = []
    stds_corrected = []
    filenames = [filename_prefix + str(x) for x in RPS]
    # For each res_<RPS>
    for filename_rps in filenames:
        latencies = []
        latencies_corrected = []
        # For each res_<RPS>_<i>
        for iteration in range(1, ITERATIONS + 1):
            filename_rps_iter = filename_rps + "_" + str(iteration)
            with open(filename_rps_iter) as log_file:
                lines = log_file.readlines()
                # Print the actual RPS
                print("Number of conns - " + NCONNS + ", Desired RPS - " + filename_rps_iter.split("_")[-2] + ", Real RPS: " + [x.split()[1] for x in lines if "Requests/sec:" in x][0] + " (" + filename_rps_iter.split("/")[-2].split("_")[-1] + ")")
                # Collect "corrected" (+ waiting time) latency
                latency_str = [x.split()[1] for x in lines if PERCENTILE in x][CORRECTED_LATENCY]
                latency = 0
                if "ms" in latency_str:
                    latency = float(latency_str[:-2])  # milliseconds
                elif "s" in latency_str:
                    latency = float(latency_str[:-1]) * 1000  # seconds
                else:
                    latency = float(latency_str[:-1]) * 60000  # minutes
                latencies_corrected.append(latency)
        # mean_latencies_corrected.append(np.mean(latencies_corrected))
        mean_latencies_corrected.append(stats.trim_mean(latencies_corrected, 0.2))
        stds_corrected.append(np.std(latencies_corrected))
    return [RPS, mean_latencies_corrected, stds_corrected]


mono_read = load_latency("/home/sergiyivan/work/polywasm/bench-results/3-one-cpu-read-final/nconns_" + NCONNS + "/benchmark_sn_res_mono/res_", RPS_READ)
micr_read = load_latency("/home/sergiyivan/work/polywasm/bench-results/3-one-cpu-read-final/nconns_" + NCONNS + "/benchmark_sn_res_micr/res_", RPS_READ)

mono_write = load_latency("/home/sergiyivan/work/polywasm/bench-results/4-one-cpu-write-populated-final/nconns_" + NCONNS + "/benchmark_sn_res_mono/res_", RPS_WRITE)
micr_write = load_latency("/home/sergiyivan/work/polywasm/bench-results/4-one-cpu-write-populated-final/nconns_" + NCONNS + "/benchmark_sn_res_micr/res_", RPS_WRITE)

matplotlib.rcParams.update({'font.size': 16})
fig, (ax1, ax2) = plt.subplots(2, 1)

plt.subplots_adjust(left=None, bottom=None, right=None, top=None, wspace=None, hspace=0.25)

ax1.plot(micr_read[0], micr_read[1], linestyle = "--", color = "C1", marker = "x", linewidth = 3, label = "Network")
ax1.plot(mono_read[0], mono_read[1], linestyle = "-",  color = "C0",   marker = "x", linewidth = 3, label = "Local")
ax1.set_ylim(ymin=0, ymax=1000)
ax1.set_xlim(xmin=0, xmax=600)
ax1.grid()

ax2.plot(micr_write[0], micr_write[1], linestyle = "--", color = "C1", marker = "x", linewidth = 3, label = "Network")
ax2.plot(mono_write[0], mono_write[1], linestyle = "-",  color = "C0",   marker = "x", linewidth = 3, label = "Local")
ax2.set_xlabel("RPS")
ax2.set_ylim(ymin=0, ymax=1000)
ax2.set_xlim(xmin=0, xmax=300)
ax2.grid()

fig.set_figwidth(10)
fig.set_figheight(4)
fig.text(0.04, 0.25, 'P99 Tail Latency (ms)', ha='center', rotation='vertical')

fig.text(0.175, 0.8, 'Read', ha='center')
fig.text(0.175, 0.37, 'Write', ha='center')

ax1.legend(ncol=1, loc='upper center')
plt.savefig("communication-overhead.pdf", bbox_inches="tight")
plt.savefig("communication-overhead.png", dpi=300, bbox_inches="tight")
