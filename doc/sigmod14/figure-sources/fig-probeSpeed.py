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
data = MyData("results/tw-data.csv", header_str="Writes");
#data.appendFile("results/vldb_experiment_bal.dat", header_str="timestamp");
# Pre-slice the Skew=0.0, Threads=32, Density=2, Selectivity=100, Outer=1e9
# Latter we bind Algorithm, Payload
data = data.filterSubset(include={'skew':0.0, 'threads':32, 'bthreads':16, 'selectivity':100,
                         'outer':1000000000, 'density':2},
                         exclude={'probe-time':None, 'numjoins':[2,3,4]})

#########################################
# START OF FIGURE


# set figure size
## 1-column width is ~3.4
## 2-column (label wide) is ~6.8
#rcParams['figure.figsize'] = 3.35, 2.2
rcParams['figure.figsize'] = 7.6, 2.3

# font size
matplotlib.rcParams.update({'font.size': 9})

## Get the entries for:
## Payload = {8,24,0}, Inner {variable}, Outer = 10^9, Threads = 32, Selectivity = 100, Density = 2, Skew = 0
## GAT, CAT, CHT, LPR
algos = ('gat','cht','cat','lpr')

def drawLinesPayload(ax, payload, showLegend=False):
    ## Prepare B payload figure
    attrs = ('c^:', 'rd-', 'yo--', 'gs-.', 'b<-', 'c>-')
    xvalues=[100,1000,10000,100000,1000000,10000000,100000000]
    plots=[]
    for algo, attr in zip(algos, attrs):
        Xs, Ys = \
            data.filterSelect(xcol='inner', ycol='probe-time', xvalues=xvalues,
                              include={'algo':algo, 'payload-size':payload})
        Ys = [1000000.0/y for y in Ys]  # Scale tuple/us
        print algo, payload, Xs, Ys
        plots.append( ax.plot(Xs, Ys, attr)[0] )
 
    ax.set_xscale('log')
    ax.set_xlim(50, 150000000)
    # If shared axis then get_ylim not avail
    #ax.set_ylim(0, ax.get_ylim()[1]*1.1) # Add pad above for appearance
    ax.set_ylim(0, 1250)
    sTitle = str(payload) + ' B Payload'
    ax.set_title(sTitle)
    ax.set_xlabel('Inner Cardinality')
    for tick in ax.xaxis.get_major_ticks():
        tick.label.set_fontsize(9)
    for tick in ax.yaxis.get_major_ticks():
        tick.label.set_fontsize(9)

    if showLegend:
        # Legend locations: [upper lower center] [left right center]  or best  or center
        algosDisp = [algo.upper() for algo in algos]
        ax.legend(plots, algosDisp, 'best', fontsize=8)
        leg = ax.get_legend()
        leg.set_frame_on(False)

    return plots


## Three plots sharing the Y-axis
fig, (ax8, ax24, ax0) = plt.subplots(1, 3, sharey=True)
fig.subplots_adjust(left=0.11,bottom=0.22,right=0.98, top=0.87,wspace=0.05)
# no timestamp
# fig.suptitle('Plot '+sys.argv[0]+' '+str(datetime.datetime.now()), fontsize=8)

plots8 = drawLinesPayload(ax8, 8)
ax8.set_ylabel('Probe speed\n(million outer/sec)', fontsize=9)

plots24 = drawLinesPayload(ax24, 24, showLegend=True)
plots0 = drawLinesPayload(ax0, 0)

MyData.MyShow(plt) # show or save plot
