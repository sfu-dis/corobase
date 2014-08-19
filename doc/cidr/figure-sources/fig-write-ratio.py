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
data100 = MyData("../silo-microbench-results/rand-100K.csv", header_str="Reads", delimiter=' ');
data200 = MyData("../silo-microbench-results/rand-200K.csv", header_str="Reads", delimiter=' ');
data300 = MyData("../silo-microbench-results/rand-300K.csv", header_str="Reads", delimiter=' ');

#########################################
# START OF FIGURE

# set figure size
## 1-column width is ~3.4
## 2-column (label wide) is ~6.8
#rcParams['figure.figsize'] = 3.35, 2.2
rcParams['figure.figsize'] = 3, 1.9

# font size
matplotlib.rcParams.update({'font.size': 9})

## Get the entries for:
writes100 = (1, 10, 100, 1000, 10000)
writes200 = (2, 20, 200, 2000, 20000)
writes300 = (3, 30, 300, 3000, 30000)
#writeratio = (0.001, 0.01, 0.1, 1, 10)
writeratio = (0.00001, 0.0001, 0.001, 0.01, 0.1)
#attrs = ('c^:', 'rd-', 'yo--', 'gs-.', 'b<-', 'c>-')
attrs = ('b^-', 'rd-', 'yo--', 'gs-.', 'b<-')

def drawLinesPayload(ax, ymax=1.5, showLegend=False):
    ## Prepare commits/aborts figure
    xvalues=[0.00001,0.0001,0.001,0.01,0.1]
    plots=[]
    plotsLabel=['100K','200K', '300K']

    #for write, attr in zip(writes, attrs):
    # Read Commits/s for 100K
    Xs, Ys = \
        data100.filterSelect(xcol='Writes', ycol='Commits/s', xvalues=writes100,
                             include={'Threads':32})
    NormYs = [Y/Ys[0] for Y in Ys]
    print "100K", Xs, Ys, NormYs
    plots.append( ax.plot(writeratio, NormYs, attrs[2])[0] )

    # Read Commits/s for 200K
    Xs, Ys = \
        data200.filterSelect(xcol='Writes', ycol='Commits/s', xvalues=writes200,
                          include={'Threads':32})
    NormYs = [Y/Ys[0] for Y in Ys]
    print "200K", Xs, Ys, NormYs
    plots.append( ax.plot(writeratio, NormYs, attrs[3])[0] )

    # Read Commits/s for 300K
    Xs, Ys = \
        data300.filterSelect(xcol='Writes', ycol='Commits/s', xvalues=writes300,
                          include={'Threads':32})
    NormYs = [Y/Ys[0] for Y in Ys]
    print "300K", Xs, Ys, NormYs
    plots.append( ax.plot(writeratio, NormYs, attrs[4])[0] )
 
    ax.set_xscale('log')
    #ax.set_xlim(0.0001, 10)
    ax.set_xticks(xvalues, minor=False)

    # If shared axis then get_ylim not avail
    #ax.set_ylim(0, ax.get_ylim()[1]*1.1) # Add pad above for appearance
    ax.set_ylim(0, ymax)

    # Print ratio in the title
    # sTitle = "Norm. perf
    # ax.set_title(sTitle, fontsize=9)
    ax.set_xlabel('Ratio of Writes/Reads')
    for tick in ax.xaxis.get_major_ticks():
        tick.label.set_fontsize(9)
    for tick in ax.yaxis.get_major_ticks():
        tick.label.set_fontsize(9)

    if showLegend:
        # Legend locations: [upper lower center] [left right center]  or best  or center
        #algosDisp = [algo.upper() for algo in algos]
        ax.legend(plots, plotsLabel, 'upper right', fontsize=8)
        leg = ax.get_legend()
        leg.set_frame_on(False)

    return plots

## Two plots sharing the Y-axis
fig, (ax) = plt.subplots(1, 1, sharey=True)
fig.subplots_adjust(left=0.15, bottom=0.22, right=0.9, top=0.87, wspace=0.4)
# no timestamp
# fig.suptitle('Plot '+sys.argv[0]+' '+str(datetime.datetime.now()), fontsize=8)

plotsNorm = drawLinesPayload(ax, showLegend=True)
ax.set_ylabel('Norm. performance', fontsize=9)

MyData.MyShow(plt) # show or save plot
