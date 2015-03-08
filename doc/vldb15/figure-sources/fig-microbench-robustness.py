#!/usr/bin/env python

# Custom reader of the catjoin results .dat file
from MyData import MyData

# basic matplotlib imports
import matplotlib
MyData.MyInit(matplotlib) # prep for X or no X
import numpy as np
import matplotlib.pyplot as plt
from pylab import *

rcParams['figure.figsize'] = 7.3, 1.9
matplotlib.rcParams.update({'font.size': 9})
attrs = ('rd-', 'b^-', 'yo--', 'gs-.', 'k<-', 'm^:')

def drawLinesPayload(ax, silo, ermia_si, ermia_ssi, xaxis, xvalues, ymax, showLegend=False):
    ## Prepare commits/aborts figure
    plots=[]
    plotsLabel=['Silo(OCC)', 'ERMIA(SI)', 'ERMIA(SSI)']

    # Read Commits/s for 100K
    Xs, Ys = \
        silo.filterSelect(xcol='Writes', ycol='Commits/s', xvalues=xvalues,
                             include={'Threads':24})
    plots.append( ax.plot(xaxis, Ys, attrs[0])[0] )

    Xs, Ys = \
        ermia_si.filterSelect(xcol='Writes', ycol='Commits/s', xvalues=xvalues,
                             include={'Threads':24})
    plots.append( ax.plot(xaxis, Ys, attrs[1])[0] )
    ax.set_xscale('log')
    ax.set_xticks(xaxis, minor=False)
    ax.set_ylim(0, ymax)

    # Print ratio in the title
    for tick in ax.xaxis.get_major_ticks():
        tick.label.set_fontsize(9)
    for tick in ax.yaxis.get_major_ticks():
        tick.label.set_fontsize(9)

    if showLegend:
        # Legend locations: [upper lower center] [left right center]  or best  or center
        #algosDisp = [algo.upper() for algo in algos]
        ax.legend(plots, plotsLabel, 'lower left', fontsize=7)
        leg = ax.get_legend()
        leg.set_frame_on(False)

    return plots

## plots not sharing the Y-axis
fig, (ax_1k, ax_10k, ax_100k ) = plt.subplots(1, 3, sharey=False)
fig.subplots_adjust(left=0.1, bottom=0.2, right=0.9, top=0.87, wspace=0.7)

# Datafile load
silo_1k = MyData("../vldb_result/microbench/silo/rand-1k.csv", header_str="Reads", delimiter=' ');
ermia_si_1k = MyData("../vldb_result/microbench/ermia-si/rand-1k.csv", header_str="Reads", delimiter=' ');
ermia_ssi_1k = MyData("../vldb_result/microbench/silo/rand-1k.csv", header_str="Reads", delimiter=' ');
silo_10k = MyData("../vldb_result/microbench/silo/rand-10k.csv", header_str="Reads", delimiter=' ');
ermia_si_10k = MyData("../vldb_result/microbench/ermia-si/rand-10k.csv", header_str="Reads", delimiter=' ');
ermia_ssi_10k = MyData("../vldb_result/microbench/silo/rand-10k.csv", header_str="Reads", delimiter=' ');
silo_100k = MyData("../vldb_result/microbench/silo/rand-100k.csv", header_str="Reads", delimiter=' ');
ermia_si_100k = MyData("../vldb_result/microbench/ermia-si/rand-100k.csv", header_str="Reads", delimiter=' ');
ermia_ssi_100k = MyData("../vldb_result/microbench/silo/rand-100k.csv", header_str="Reads", delimiter=' ');

#Plots
plots = drawLinesPayload(ax_1k, silo_1k, ermia_si_1k, ermia_ssi_1k, (0.001,0.01,0.1),(1,10,100),100000, showLegend=True)
ax_1k.set_title("1k reads(24 threads)", fontsize=9)
ax_1k.set_ylabel('Throughput(tps)', fontsize=9)

plots = drawLinesPayload(ax_10k, silo_10k, ermia_si_10k, ermia_ssi_10k, (0.001,0.01,0.1),(10,100,1000),8000, showLegend=False)
ax_10k.set_title("10k reads(24 threads)", fontsize=9)
ax_10k.set_xlabel('Ratio of Writes/Reads')
ax_10k.set_ylabel('Throughput (tps)', fontsize=9)

plots = drawLinesPayload(ax_100k, silo_100k, ermia_si_100k, ermia_ssi_100k, (0.001,0.01,0.1),(100,1000,10000),500, showLegend=False)
ax_100k.set_title("100k reads(24 threads)", fontsize=9)
ax_100k.set_ylabel('Throughput (tps)', fontsize=9)

#Show
MyData.MyShow(plt) # show or save plot
