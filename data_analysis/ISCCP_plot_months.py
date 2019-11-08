#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import iris
from glob import glob
import matplotlib.pyplot as plt
import matplotlib.patches as patch
import numpy as np
import cartopy.crs as ccrs
from cis.data_io.gridded_data import make_from_cube

def load_callback(cube, field, fname):
    variable_attributes = ['date_metadata_modified', 'date_issued', 'date_modified', 'date_created',
                           'isccp_input_files', 'time_coverage_start', 'time_coverage_end', 'isccp_month', 'id',
                           'history']
    for a in variable_attributes:
        cube.attributes.pop(a)
months=['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']       
for i in  range(12):
    directory = '/Users/clim_proc/Documents/POC MPhys Project/Modis/ISCCP data/data/international-satellite-cloud-climate-project-isccp-h-series-data/access/isccp-basic/hgm/'
    filelist = glob(directory+'ISCCP-Basic*GLOBAL*0'+str(i+1)+'*99.9999*')
    c = iris.load(filelist[0], iris.Constraint(cube_func=lambda x:
        x.var_name=='cldamt_types'), callback=load_callback).concatenate_cube()[:,1,...]
    no = 0
    for filename in filelist[1:]:
    
        year = int(filename.split('.')[4])
        c += (iris.load(filename, iris.Constraint(cube_func=lambda x:
            x.var_name=='cldamt_types'), callback=load_callback).concatenate_cube())[:,1,...]
        no += 1
        print(no+1, ' of ', len(filelist), ' done')
        
    mean = c/no
    fig=plt.figure(figsize=(15,15))#, dpi=300)
    ax = plt.axes(projection=ccrs.PlateCarree())
    ax.coastlines()
    ax.set_title=(months[i])
    make_from_cube(c).plot(ax=ax)
    plt.savefig('./figures/cloud_amount_'+str(i), dpi=300)
    print(months[i] + ' done.')
        