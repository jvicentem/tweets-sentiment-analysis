# -*- coding: utf-8 -*-
"""
Created on Thu Nov 24 15:53:21 2016

@author: jkobe
"""

from __future__ import print_function
import numpy as np
import matplotlib.pyplot as plt
from mpl_toolkits.basemap import Basemap as Basemap
from matplotlib.colors import rgb2hex
from matplotlib.patches import Polygon


# Lambert Conformal map of lower 48 states.
m = Basemap(llcrnrlon=-119,llcrnrlat=22,urcrnrlon=-64,urcrnrlat=49,
            projection='lcc',lat_1=33,lat_2=45,lon_0=-95)
# draw state boundaries using a ShapeFile
shp_info = m.readshapefile('st99_d00','states',drawbounds=True)


happiness = dict()
file = open("emaitzak2")
lines = file.readlines()
lines = lines[:-11]
for linea in lines:
    state, hap = linea.split("\t")
    happiness[state] = int(hap)

# choose a color for each state based on population density.
colors={}
statenames=[]
cmap = plt.cm.winter # use 'winter' colormap
vmin = min(happiness.values()); vmax = max(happiness.values()) # set range.
#print(m.states_info[0].keys())
for shapedict in m.states_info:
    # shapedict is a dictionary with information of each state. Each state can
    # have more than  one.
    statename = shapedict['NAME']
    # skip DC and Puerto Rico.
    if statename not in ['District of Columbia','Puerto Rico','American Samoa', 'Guam','Hawaii']:
        hap = happiness[statename] # we take the happiness of each state
        # calling colormap with value between 0 and 1 returns
        # rgba value.  Invert color range (hot colors are high
        # population), take sqrt root to spread out colors more.
        colors[statename] = cmap(1-np.sqrt((hap-vmin)/(vmax-vmin)))[:3] 
        # cmap has an scale of winter colors between 0 and 1. Colors near to
        # 0 are blue and near to 1 are green. Colors are given in RGB triplet.
        # We scale values between [0,1]. Most frequent states near to 0, blue.
        # Less frequent states near to 1, yellow-green
    statenames.append(statename) # we create a list of states to colour them
    # after
# cycle through state names, color each one.
ax = plt.gca() # get current axes instance
for nshape,seg in enumerate(m.states):
    # m.state indicates the area of each state,which is taken by seg
    # nshape the position in the list
    # skip DC and Puerto Rico.
    if statenames[nshape] not in ['District of Columbia','Puerto Rico','American Samoa', 'Guam','Hawaii']:
#        print(rgb2hex(colors[statenames[nshape]]))   

        color = rgb2hex(colors[statenames[nshape]]) # convert colors from
        # RGB to HEX
        poly = Polygon(seg,facecolor=color,edgecolor=color) # Paint the given 
        # state and border by seg coords with the HEX color
        ax.add_patch(poly)
        
# draw meridians and parallels.
m.drawparallels(np.arange(25,65,20),labels=[1,0,0,0])
m.drawmeridians(np.arange(-120,-40,20),labels=[0,0,0,1])
plt.title('Happiness by State')
plt.show()
