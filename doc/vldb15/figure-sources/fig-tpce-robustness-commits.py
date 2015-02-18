#!/usr/bin/env python
from MyData import MyData
import matplotlib
MyData.MyInit(matplotlib) # prep for X or no X
import numpy as np
import matplotlib.pyplot as plt
from pylab import *

# Load the results
ermia_si = MyData("../vldb_result/ermia-si.csv", header_str="system", delimiter=',');
ermia_ssi = MyData("../vldb_result/ermia-ssi.csv", header_str="system", delimiter=',');
silo = MyData("../vldb_result/silo-native.csv", header_str="system", delimiter=',');
ind = np.arange(3)
width = 0.35
rcParams['figure.figsize'] = 6.9, 2.4
matplotlib.rcParams.update({'font.size': 9})

def drawLinesPayload(ax, bench, ycol, ymax=10000, showLegend=True):
    xvalues=['tpce10', 'tpce20', 'tpce40']
    plots=[]
    plotsLabel=['ERMIA-SI','ERMIA-SSI','SILO']

    Xs, Ys = \
        ermia_si.filterSelect(xcol='bench', ycol=ycol, xvalues=xvalues,
                          include={'threads':24})
    Ys[:] = [ y/40 for y in Ys]
    print Xs, Ys

    plots.append( ax.bar( ind, Ys, width, color='r'))

    Xs, Ys = \
        ermia_ssi.filterSelect(xcol='bench', ycol=ycol, xvalues=xvalues,
                          include={'threads':24})
    Ys[:] = [ y/40 for y in Ys]
    print Xs, Ys
    plots.append( ax.bar( ind+width, Ys, width, color='g'))

    Xs, Ys = \
        silo.filterSelect(xcol='bench', ycol=ycol, xvalues=xvalues,
                          include={'threads':24})
    Ys[:] = [ y/40 for y in Ys]
    print Xs, Ys
    plots.append( ax.bar( ind+width+width, Ys, width, color='b'))

    ax.set_xticks( ind + width )
    ax.set_xticklabels(('10%', '20%', '40%'), minor=False)

    # If shared axis then get_ylim not avail
    ax.set_ylim(0, ymax)

    # Print ratio in the title
#    ax.set_xlabel('Contention Degree ')
#    for tick in ax.xaxis.get_major_ticks():
#        tick.label.set_fontsize(9)
#    for tick in ax.yaxis.get_major_ticks():
#        tick.label.set_fontsize(9)

    if showLegend:
        # Legend locations: [upper lower center] [left right center]  or best  or center
        #algosDisp = [algo.upper() for algo in algos]
        ax.legend(plots, plotsLabel, 'upper right')
        leg = ax.get_legend()
        leg.set_frame_on(False)

    return plots

f,(ax_1,ax_2) = plt.subplots(1,2,sharey=False)
f.subplots_adjust(left=0.12, bottom=0.22, right=0.98, top=0.87, wspace=0.4)
drawLinesPayload(ax_1, 'tpcc_contention', 'total_commits', 15000, False)
drawLinesPayload(ax_2, 'tpcc_org', 'total_commits', 15000)
ax_1.set_ylabel('Throughput (Tps)', fontsize=9)
ax_2.set_ylabel('Throughput (Tps)', fontsize=9)
MyData.MyShow(plt) # show or save plot
