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
data1   = MyData("../vldb_result/rand-1k.csv", header_str="Reads", delimiter=' ');
data10  = MyData("../vldb_result/rand-10k.csv", header_str="Reads", delimiter=' ');
data100 = MyData("../vldb_result/rand-100k.csv", header_str="Reads", delimiter=' ');
data200 = MyData("../vldb_result/rand-200k.csv", header_str="Reads", delimiter=' ');
data300 = MyData("../vldb_result/rand-300k.csv", header_str="Reads", delimiter=' ');
dataTPCC = MyData("../vldb_result/silo-tpcc.csv", header_str="LOG", delimiter=' ');

#########################################
# START OF FIGURE

# set figure size
## 1-column width is ~3.4
## 2-column (label wide) is ~6.8
#rcParams['figure.figsize'] = 3.35, 2.2
rcParams['figure.figsize'] = 7.3, 1.9

# font size
matplotlib.rcParams.update({'font.size': 9})

## Get the entries for:
writes1 = (1, 10, 100)
writes10 = (1, 10, 100, 1000)
writes100 = (1, 10, 100, 1000, 10000)
writes200 = (2, 20, 200, 2000, 20000)
writes300 = (3, 30, 300, 3000, 30000)
#writeratio = (0.001, 0.01, 0.1, 1, 10)
writeratio = (0.00001, 0.0001, 0.001, 0.01, 0.1)
writeratio1 = (0.001, 0.01, 0.1)
writeratio10 = (0.0001, 0.001, 0.01, 0.1)
#attrs = ('c^:', 'rd-', 'yo--', 'gs-.', 'b<-', 'c>-')
attrs = ('b^-', 'rd-', 'yo--', 'gs-.', 'k<-', 'm^:')

def drawLinesPayload(ax, ymax=1.5, showLegend=False):
    ## Prepare commits/aborts figure
    xvalues=[0.00001,0.0001,0.001,0.01,0.1]    
    plots=[]
    plotsLabel=['1K', '10K', '100K','200K', '300K']

    # Read Commits/s for 100K
    Xs, Ys = \
        data1.filterSelect(xcol='Writes', ycol='Commits/s', xvalues=writes1,
                             include={'Threads':32})
    NormYs = [Y/Ys[0] for Y in Ys]
    print "1K", Xs, Ys, NormYs
    plots.append( ax.plot(writeratio1, NormYs, attrs[0])[0] )

    # Read Commits/s for 10K
    Xs, Ys = \
        data10.filterSelect(xcol='Writes', ycol='Commits/s', xvalues=writes10,
                             include={'Threads':32})
    NormYs = [Y/Ys[0] for Y in Ys]
    print "100K", Xs, Ys, NormYs
    plots.append( ax.plot(writeratio10, NormYs, attrs[1])[0] )

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
    ax.set_title("Read/write benchmark (32 threads)", fontsize=9)
    ax.set_xlabel('Ratio of Writes/Reads')
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


def drawTPCC(ax, ymax=650, showLegend=True):
    ## Prepare commits/aborts figure
    xvalues=[32, 16, 8, 4]
    plots=[]
    plotsLabel=['Commits','Aborts', 'Commits no log','Aborts no log']

    # Read TPS w. logging
    Xs, Ys = \
        dataTPCC.filterSelect(xcol='sf', ycol='tps', xvalues=xvalues, include={'LOG':1})
    NormYs = [Y/1000 for Y in Ys]    
    print "Silo tps", Xs, Ys, NormYs
    plots.append( ax.plot(xvalues, NormYs, 'b^-')[0] )

    # Read Aborts w. logging
    Xs, Ys = \
        dataTPCC.filterSelect(xcol='sf', ycol='abt/s', xvalues=xvalues, include={'LOG':1})
    NormYs = [Y/1000 for Y in Ys]    
    print "Silo abt", Xs, Ys, NormYs
    plots.append( ax.plot(xvalues, NormYs, 'rd-')[0] )

    # Read TPS w/o logging
    Xs, Ys = \
        dataTPCC.filterSelect(xcol='sf', ycol='tps', xvalues=xvalues, include={'LOG':0})
    NormYs = [Y/1000 for Y in Ys]    
    print "Silo w/o log tps", Xs, Ys, NormYs
    plots.append( ax.plot(xvalues, NormYs, 'b^:')[0] )

    # Read Aborts w. logging
    Xs, Ys = \
        dataTPCC.filterSelect(xcol='sf', ycol='abt/s', xvalues=xvalues, include={'LOG':0})
    NormYs = [Y/1000 for Y in Ys]    
    print "Silo w/o logging abt", Xs, Ys, NormYs
    plots.append( ax.plot(xvalues, NormYs, 'rd:')[0] )

 
    #ax.set_xscale('log')
    ax.set_xlim(32, 4)
    ax.set_xticks(xvalues, minor=False)

    # If shared axis then get_ylim not avail
    #ax.set_ylim(0, ax.get_ylim()[1]*1.1) # Add pad above for appearance
    ax.set_ylim(0, ymax)
    # Print ratio in the title
    sTitle = "TPC-C w. 32 clients"
    ax.set_title(sTitle, fontsize=9)
    ax.set_xlabel('# Warehouses')
    for tick in ax.xaxis.get_major_ticks():
        tick.label.set_fontsize(9)
    for tick in ax.yaxis.get_major_ticks():
        tick.label.set_fontsize(9)

    if showLegend:
        # Legend locations: [upper lower center] [left right center]  or best  or center
        #algosDisp = [algo.upper() for algo in algos]
        # , bbox_to_anchor=(1, 0.5))
        ax.legend(plots, plotsLabel, fontsize=7,loc='center left')
        leg = ax.get_legend()
        leg.set_frame_on(False)

    return plots


## Two plots not sharing the Y-axis
fig, (ax, axTPCC) = plt.subplots(1, 2, sharey=False)
fig.subplots_adjust(left=0.1, bottom=0.2, right=0.9, top=0.87, wspace=0.7)
# no timestamp
# fig.suptitle('Plot '+sys.argv[0]+' '+str(datetime.datetime.now()), fontsize=8)

plotsTPCC = drawTPCC(axTPCC, showLegend=True)
axTPCC.set_ylabel('Transaction rate (KTps)', fontsize=9)

plotsNorm = drawLinesPayload(ax, showLegend=True)
ax.set_ylabel('Norm. throughput', fontsize=9)

MyData.MyShow(plt) # show or save plot
