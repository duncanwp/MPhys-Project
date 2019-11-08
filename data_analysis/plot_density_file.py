#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import cis
from glob import glob
import xarray as xr
from cis.data_io.gridded_data import make_from_cube
import matplotlib.pyplot as plt
from cartopy import crs as ccrs
import matplotlib.patches as patch

dataDIR = './density_data/*count*'
filelist = glob(dataDIR)
counts=[]
for file in filelist:
    counts.append(cis.read_data(file, 'poc_mask_num_points'))
total_counts = make_from_cube(sum(counts))
    
dataDIR = './density_data/*sum*'
filelist = glob(dataDIR)
sums=[]
for file in filelist:
    sums.append(cis.read_data(file, 'poc_mask'))
total_sum = make_from_cube(sum(sums))

lon = {'cal':[230,240], 'per':[270,280], 'nam':[360,10]}
lat = {'cal':[20,30], 'per':[-20,-10], 'nam':[-20,-10]}

fig=plt.figure(figsize=(15,15))#, dpi=300)
ax = plt.axes(projection=ccrs.PlateCarree())
ax.coastlines()
total_counts.plot(cmap='cool', ax=ax)
for place, coords in lat.items():
    if place == 'nam':
        rect = patch.Rectangle((lon[place][0],lat[place][0]), 360-lon[place][0]+lon[place][1], lat[place][1]-lat[place][0], transform=ccrs.PlateCarree(), linewidth=1, edgecolor='r', facecolor='none')
        ax.add_patch(rect)
    else:
        rect = patch.Rectangle((lon[place][0],lat[place][0]), lon[place][1]-lon[place][0], lat[place][1]-lat[place][0], transform=ccrs.PlateCarree(), linewidth=1, edgecolor='r', facecolor='none')
        ax.add_patch(rect)
plt.show()

fig=plt.figure(figsize=(15,15))#, dpi=300)
ax = plt.axes(projection=ccrs.PlateCarree())
ax.coastlines()
total_sum.plot(cmap='YlGnBu', ax=ax)
for place, coords in lat.items():
    if place == 'nam':
        rect = patch.Rectangle((lon[place][0],lat[place][0]), 360-lon[place][0]+lon[place][1], lat[place][1]-lat[place][0], transform=ccrs.PlateCarree(), linewidth=1, edgecolor='r', facecolor='none')
        ax.add_patch(rect)
    else:
        rect = patch.Rectangle((lon[place][0],lat[place][0]), lon[place][1]-lon[place][0], lat[place][1]-lat[place][0], transform=ccrs.PlateCarree(), linewidth=1, edgecolor='r', facecolor='none')
        ax.add_patch(rect)
plt.show()

fig=plt.figure(figsize=(15,15))#, dpi=300)
ax = plt.axes(projection=ccrs.PlateCarree())
ax.coastlines()
make_from_cube(total_sum/total_counts).plot(ax=ax)
plt.show()

good_colours = ['cool', 'Greens', 'Reds', 'Blues', 'YlGnBu', 'YlOrRd']