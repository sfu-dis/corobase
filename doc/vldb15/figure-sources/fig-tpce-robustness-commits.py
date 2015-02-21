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
ind = np.arange(3)
width = 0.20
rcParams['figure.figsize'] = 6.9, 2.4
matplotlib.rcParams.update({'font.size': 9})

def drawBarsPayload(ax, ycol, ymax=10000, showLegend=True):
    xvalues=['tpce5', 'tpce10', 'tpce20']
    plots=[]
    plotsLabel=['ERMIA-SI','ERMIA-SSI','SILO']

    Xs, Ys = \
        ermia_si.filterSelect(xcol='bench', ycol=ycol, xvalues=xvalues,
                          include={'threads':24})
    Ys[:] = [ y/40 for y in Ys]
    print Xs, Ys
    plots.append( ax.bar( ind + 0.2, Ys, width, color='r'))

    Xs, Ys = \
        ermia_ssi.filterSelect(xcol='bench', ycol=ycol, xvalues=xvalues,
                          include={'threads':24})
    Ys[:] = [ y/40 for y in Ys]
    print Xs, Ys
    plots.append( ax.bar( ind+width+0.2, Ys, width, color='y'))

    Xs, Ys = \
        silo.filterSelect(xcol='bench', ycol=ycol, xvalues=xvalues,
                          include={'threads':24})
    Ys[:] = [ y/40 for y in Ys]
    print Xs, Ys
    plots.append( ax.bar( ind+width+width+0.2, Ys, width, color='b'))

    ax.set_xticks( ind + 0.2 + (width*1.5) )
    ax.set_xticklabels(('5%', '10%', '20%'), minor=False)

    # If shared axis then get_ylim not avail
    ax.set_ylim(0, ymax)

    if showLegend:
        # Legend locations: [upper lower center] [left right center]  or best  or center
        #algosDisp = [algo.upper() for algo in algos]
        ax.legend(plots, plotsLabel, 'upper right')
        leg = ax.get_legend()
        leg.set_frame_on(False)

    return plots

f,(ax_1,ax_2) = plt.subplots(1,2,sharey=False)
f.subplots_adjust(left=0.12, bottom=0.22, right=0.98, top=0.87, wspace=0.4)
drawBarsPayload(ax_1, 'total_commits', 15000, False)
drawBarsPayload(ax_2, 'total_query_commits', 1500)
ax_1.set_ylabel('Throughput (Tps)', fontsize=9)
ax_2.set_ylabel('Throughput (Tps)', fontsize=9)
MyData.MyShow(plt) # show or save plot
