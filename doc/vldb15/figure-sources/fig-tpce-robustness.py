#!/usr/bin/env python
from operator import truediv
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
ind = np.arange(6)
width = 0.20
rcParams['figure.figsize'] = 6.9, 2.4
matplotlib.rcParams.update({'font.size': 9})

def drawBarsPayload(ax, ycol, showLegend=True):
    xvalues=['tpce1', 'tpce5', 'tpce10', 'tpce20', 'tpce40', 'tpce60']
    plots=[]
    plotsLabel=['ERMIA-SI','ERMIA-SSI','Silo']

    X1, Y1 = \
        ermia_si.filterSelect(xcol='bench', ycol=ycol, xvalues=xvalues,
                          include={'threads':24})
    Y1[:] = [ y/40 for y in Y1]

    X2, Y2 = \
        ermia_ssi.filterSelect(xcol='bench', ycol=ycol, xvalues=xvalues,
                          include={'threads':24})
    Y2[:] = [ y/40 for y in Y2]

    X3, Y3 = \
        silo.filterSelect(xcol='bench', ycol=ycol, xvalues=xvalues,
                          include={'threads':24})
    Y3[:] = [ y/40 for y in Y3]

    plots.append( ax.bar( ind + 0.2 , map(truediv, Y1, Y1), width, color='b'))
    plots.append( ax.bar( ind+width + 0.2, map(truediv,Y2, Y1), width, color='r'))
    plots.append( ax.bar( ind+width+width + 0.2, map(truediv,Y3,Y1), width, color='y'))

    ax.set_xticks( ind + 0.2+ (width*1.5) )
    ax.set_xticklabels(('1%', '5%', '10%', '20%', '40%', '60%' ), minor=False)

    # If shared axis then get_ylim not avail
    ax.set_ylim(0, 1.5)

    # Print ratio in the title
    ax.set_xlabel('Contention degree')
    for tick in ax.xaxis.get_major_ticks():
        tick.label.set_fontsize(9)
    for tick in ax.yaxis.get_major_ticks():
        tick.label.set_fontsize(9)

    if showLegend:
        ax.legend(plots, plotsLabel, 'upper right', prop={'size':7})
        leg = ax.get_legend()
        leg.set_frame_on(False)

    return plots

f,(ax_1,ax_2) = plt.subplots(1,2,sharey=False)
f.subplots_adjust(left=0.12, bottom=0.22, right=0.98, top=0.87, wspace=0.4)
drawBarsPayload(ax_1, 'total_commits',  True )
drawBarsPayload(ax_2, 'total_query_commits', False)
ax_1.set_ylabel('Throughput (tps)', fontsize=9)
ax_2.set_ylabel('Throughput (queries/s)', fontsize=9)
MyData.MyShow(plt) # show or save plot
