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
#for i in  range(12):
directory = '/Users/clim_proc/Documents/POC MPhys Project/Modis/ISCCP data/data/international-satellite-cloud-climate-project-isccp-h-series-data/access/isccp-basic/hgm/'
filelist = glob(directory+'ISCCP-Basic*GLOBAL*0'+'*99.9999*')
c = iris.load(filelist[0], iris.Constraint(cube_func=lambda x:
    x.var_name=='cldamt_types'), callback=load_callback).concatenate_cube()[:,1,...]

counts = make_from_cube(c).make_new_with_same_coordinates(data=None, var_name='mean_strat', standard_name='mean_strat',
                                       long_name='mean_stratocumulus', history='what', units='1',
                                        flatten=False)
sums = make_from_cube(c).make_new_with_same_coordinates(data=None, var_name='mean_strat', standard_name='mean_strat',
                                       long_name='mean_stratocumulus', history='what', units='percent',
                                        flatten=False)

no = 0
for filename in filelist[1:]:
    
    c = (iris.load(filename, iris.Constraint(cube_func=lambda x:
        x.var_name=='cldamt_types'), callback=load_callback).concatenate_cube())[:,1,...]
    temp_counts.data = 1-np.ma.getmask(c.data).astype('uint16')
    c.data=np.ma.filled(c.data, fill_value=0)
    
    sums += c
    counts += temp_counts
    
    no += 1
    print(no+1, ' of ', len(filelist), ' done')
        
mean = sums/counts
fig=plt.figure(figsize=(15,15))#, dpi=300)
ax = plt.axes(projection=ccrs.PlateCarree())
ax.coastlines()
make_from_cube(mean).plot(ax=ax)
plt.savefig('./figures/cloud_amount', dpi=300)
        