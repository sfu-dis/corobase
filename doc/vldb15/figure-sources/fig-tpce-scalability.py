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
silo = MyData("../vldb_result/silo.csv", header_str="system", delimiter=',');

rcParams['figure.figsize'] = 6.9, 2.4
matplotlib.rcParams.update({'font.size': 9})
attrs = ('b^-', 'rd-', 'yo--', 'gs-.', 'b<-')

def drawLinesPayload(ax, bench, ycol, ymax=10000, divide=1,  showLegend=True):
    xvalues=[1,6,12,18,24]
    plots=[]
    plotsLabel=['ERMIA-SI','ERMIA-SSI','Silo']

    Xs, Ys = \
        ermia_si.filterSelect(xcol='threads', ycol=ycol, xvalues=xvalues,
                          include={'bench':bench})
    Ys[:] = [ y/40/divide for y in Ys]
    print Xs, Ys
    plots.append( ax.plot(Xs, Ys, attrs[0])[0] )

    Xs, Ys = \
        ermia_ssi.filterSelect(xcol='threads', ycol=ycol, xvalues=xvalues,
                          include={'bench':bench})
    Ys[:] = [ y/40/divide for y in Ys]
    print Xs, Ys
    plots.append( ax.plot(Xs, Ys, attrs[1])[0] )

    Xs, Ys = \
        silo.filterSelect(xcol='threads', ycol=ycol, xvalues=xvalues,
                          include={'bench':bench})
    Ys[:] = [ y/40/divide for y in Ys]
    print Xs, Ys
    plots.append( ax.plot(Xs, Ys, attrs[2])[0] )

    #ax.set_xscale('log')
    #ax.set_xlim(4, 32)
    ax.set_xticks(xvalues, minor=False)

    # If shared axis then get_ylim not avail
    #ax.set_ylim(0, ax.get_ylim()[1]*1.1) # Add pad above for appearance
    ax.set_ylim(0, ymax)

    # Print ratio in the title
    ax.set_xlabel('# Threads')
    for tick in ax.xaxis.get_major_ticks():
        tick.label.set_fontsize(9)
    for tick in ax.yaxis.get_major_ticks():
        tick.label.set_fontsize(9)

    if showLegend:
        # Legend locations: [upper lower center] [left right center]  or best  or center
        #algosDisp = [algo.upper() for algo in algos]
        #ax.legend(plots, plotsLabel, 'upper left', fontsize=8)
        ax.legend(plots, plotsLabel, 'upper left')
        leg = ax.get_legend()
        leg.set_frame_on(False)

    return plots

f,(ax_1,ax_2) = plt.subplots(1,2,sharey=False)
f.subplots_adjust(left=0.12, bottom=0.22, right=0.98, top=0.87, wspace=0.4)
drawLinesPayload(ax_1, 'tpce20', 'total_commits', 2000, 1, True )
drawLinesPayload(ax_2, 'tpce_org', 'total_commits', 80000, 1,  False)
ax_1.set_ylabel('Throughput (Tps)', fontsize=9)
ax_2.set_ylabel('Throughput (Tps)', fontsize=9)
MyData.MyShow(plt) # show or save plot
