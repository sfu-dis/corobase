#!/usr/bin/env python

# Custom reader of the catjoin results .dat file
from MyData import MyData

# basic matplotlib imports
import matplotlib
MyData.MyInit(matplotlib) # prep for X or no X
import numpy as np
import matplotlib.pyplot as plt
from pylab import *

# Load the results
data = MyData("../silo-microbench-results/rand-300K.csv", header_str="Reads", delimiter=' ');

#########################################
# START OF FIGURE

# set figure size
## 1-column width is ~3.4
## 2-column (label wide) is ~6.8
#rcParams['figure.figsize'] = 3.35, 2.2
rcParams['figure.figsize'] = 6.9, 1.9

# font size
matplotlib.rcParams.update({'font.size': 9})

## Get the entries for:
writes = (3, 30, 300, 3000, 30000)
#attrs = ('c^:', 'rd-', 'yo--', 'gs-.', 'b<-', 'c>-')
attrs = ('b^-', 'rd-', 'yo--', 'gs-.', 'b<-')

def drawLinesPayload(ax, write, ymax=100, showLegend=False):
    ## Prepare commits/aborts figure
    xvalues=[4,8,16,24,32]
    plots=[]
    plotsLabel=['Commits','Aborts']

    #for write, attr in zip(writes, attrs):
    # Read Commits/s
    Xs, Ys = \
        data.filterSelect(xcol='Threads', ycol='Commits/s', xvalues=xvalues,
                          include={'Writes':write})
    print write, Xs, Ys
    plots.append( ax.plot(Xs, Ys, attrs[0])[0] )

    # Read Commits/s
    Xs, Ys = \
        data.filterSelect(xcol='Threads', ycol='Aborts/s', xvalues=xvalues,
                          include={'Writes':write})
    print write, Xs, Ys
    plots.append( ax.plot(Xs, Ys, attrs[1])[0] )
 
    #ax.set_xscale('log')
    #ax.set_xlim(4, 32)
    ax.set_xticks(xvalues, minor=False)

    # If shared axis then get_ylim not avail
    #ax.set_ylim(0, ax.get_ylim()[1]*1.1) # Add pad above for appearance
    ax.set_ylim(0, ymax)

    # Print ratio in the title
    ratio = double(write)/double(3000);
    sTitle = str("{0:.3f}".format(ratio)) + '% writes'
    ax.set_title(sTitle, fontsize=9)
    ax.set_xlabel('# Threads')
    for tick in ax.xaxis.get_major_ticks():
        tick.label.set_fontsize(9)
    for tick in ax.yaxis.get_major_ticks():
        tick.label.set_fontsize(9)

    if showLegend:
        # Legend locations: [upper lower center] [left right center]  or best  or center
        #algosDisp = [algo.upper() for algo in algos]
        ax.legend(plots, plotsLabel, 'upper left', fontsize=8)
        leg = ax.get_legend()
        leg.set_frame_on(False)

    return plots

## Three plots sharing the Y-axis
fig, (ax0001, ax001, ax01, ax1, ax10) = plt.subplots(1, 5, sharey=False)
fig.subplots_adjust(left=0.08, bottom=0.22, right=0.98, top=0.87, wspace=0.4)
# no timestamp
# fig.suptitle('Plot '+sys.argv[0]+' '+str(datetime.datetime.now()), fontsize=8)

plots0001 = drawLinesPayload(ax0001, writes[0])
ax0001.set_ylabel('Throughput (KTps)', fontsize=9)

plots001 = drawLinesPayload(ax001, writes[1])
plots01 = drawLinesPayload(ax01, writes[2], showLegend=True)
plots1 = drawLinesPayload(ax1, writes[3], 10)
plots10 = drawLinesPayload(ax10, writes[4], 1)

MyData.MyShow(plt) # show or save plot
