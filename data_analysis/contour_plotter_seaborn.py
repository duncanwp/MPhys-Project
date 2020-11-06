#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import pickle
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import math
import matplotlib
import os

#from matplotlib import rc
#rc('font',**{'family':'serif','sans-serif':['Computer Modern Roman']})
### for Palatino and other serif fonts use:
##rc('font',**{'family':'serif','serif':['Palatino']})
#rc('text', usetex=True)

matplotlib.rcParams.update({'font.size': 16})

with open('/gws/nopw/j04/impala/users/dwatsonparris/POC_analysis/results/results_1km.pickle', 'rb') as handle:
    data = pickle.load(handle)

#print(data[1]['latitude'])

pixel_size=1 # 1 for 1km, 5 for 5km
stripe_width=15 # No. of stripes
    
lon = {'cal':[-155,-95], 'per':[-115,-55], 'nam':[-25,35]}
lat = {'cal':[5,55], 'per':[-45,5], 'nam':[-45,15]}
name_convert = {'Peru':'per', 'California':'cal', 'Namibia':'nam'}

length = len(data[6]['latitude'][0])

pandas_data = {}
for key, value in data[6].items():
    pandas_data[key] = np.zeros((0,length))

for image in data:
    if image: 
        for variable, array in image.items():
            pandas_data[variable] = np.append(pandas_data[variable], np.asarray(array), axis=0)
            pandas_data[variable] = pd.DataFrame(pandas_data[variable])

pandas_long_form = pd.DataFrame()
for variable, array in pandas_data.items():
    pandas_long_form[variable] = array.stack()
pandas_long_form['Distance from Boundary (km)'] = (pandas_long_form.index.get_level_values(1).values-(math.ceil(length/2)-0.5))*pixel_size*stripe_width

place_list = []
for i in range(pandas_long_form.shape[0]):
    latitude = pandas_long_form['latitude'].iloc[i]
    longitude = pandas_long_form['longitude'].iloc[i]
    test=0
    for place, value in lon.items():
        if latitude > lat[place][0] and latitude < lat[place][1] and longitude >lon[place][0] and longitude < lon[place][1]:
            place_list.append(place)
            test+=1
    if test!=1:
        print('summin went wrong ere!')
        print(test)

pandas_long_form['location'] = place_list

to_plot_1km = ['Cloud_Effective_Radius', 'Cloud_Water_Path', 'Cloud_Optical_Thickness']
to_plot_5km = ['Surface_Pressure', 'Cloud_Top_Height', 'Cloud_Fraction', 'Tropopause_Height', 'Surface_Temperature']
formatted_names = {'Cloud_Effective_Radius':'Cloud Effective Radius (microns)', 'Cloud_Water_Path':'Cloud Water Path (g/m^2)', 'Cloud_Optical_Thickness':'Cloud Optical Thickness (no units)', 'Surface_Pressure':'Surface Pressure (hPa)', 'Cloud_Top_Height':'Cloud Top Height (m)', 'Cloud_Fraction':'Cloud Fraction (no units)', 'Tropopause_Height':'Tropopause Height (hPa)', 'Surface_Temperature':'Surface Temperature (K)'}
no_variables=8
index = 0
fig, ax = plt.subplots(math.floor(no_variables/2), 2, figsize = (15, 5*math.floor(no_variables/2)))#, dpi=300)
for variable in to_plot_1km:
    current_ax=ax[math.floor(index/2),index%2]
    sns.lineplot(x='Distance from Boundary (km)', y=variable, data=pandas_long_form, hue='location', ax=current_ax, ci='sd')
    current_ax.axvline(x=0, linestyle=':')
    if math.ceil((index+1)/2) != math.floor(no_variables/2):
        current_ax.set_xlabel('')
    current_ax.set_ylabel(formatted_names[variable])
    handles, labels = current_ax.get_legend_handles_labels()
    current_ax.get_legend().remove()
    index += 1
    
    
with open('/gws/nopw/j04/impala/users/dwatsonparris/POC_analysis/results/results_5km.pickle', 'rb') as handle:
    data = pickle.load(handle)
    
pixel_size=5
stripe_width=3
    
lon = {'cal':[-155,-95], 'per':[-115,-55], 'nam':[-25,35]}
lat = {'cal':[5,55], 'per':[-45,5], 'nam':[-45,15]}
name_convert = {'Peru':'per', 'California':'cal', 'Namibia':'nam'}

length = len(data[6]['latitude'][0])

pandas_data = {}
for key, value in data[6].items():
    pandas_data[key] = np.zeros((0,length))

for image in data:
    if image: 
        for variable, array in image.items():
            pandas_data[variable] = np.append(pandas_data[variable], np.asarray(array), axis=0)
            pandas_data[variable] = pd.DataFrame(pandas_data[variable])

pandas_long_form = pd.DataFrame()
for variable, array in pandas_data.items():
    pandas_long_form[variable] = array.stack()
pandas_long_form['Distance from Boundary (km)'] = (pandas_long_form.index.get_level_values(1).values-(math.ceil(length/2)-0.5))*pixel_size*stripe_width

place_list = []
for i in range(pandas_long_form.shape[0]):
    latitude = pandas_long_form['latitude'].iloc[i]
    longitude = pandas_long_form['longitude'].iloc[i]
    test=0
    for place, value in lon.items():
        if latitude > lat[place][0] and latitude < lat[place][1] and longitude >lon[place][0] and longitude < lon[place][1]:
            place_list.append(place)
            test+=1
    if test!=1:
        print('summin went wrong ere!')
        print(test)

pandas_long_form['location'] = place_list

to_plot_1km = ['Cloud_Effective_Radius', 'Cloud_Water_Path', 'Cloud_Optical_Thickness']
to_plot_5km = ['Surface_Pressure', 'Cloud_Top_Height', 'Cloud_Fraction', 'Tropopause_Height', 'Surface_Temperature']
no_variables=8
for variable in to_plot_5km:
    current_ax=ax[math.floor(index/2),index%2]
    sns.lineplot(x='Distance from Boundary (km)', y=variable, data=pandas_long_form, hue='location', ax=current_ax, ci='sd')
    current_ax.axvline(x=0, linestyle=':')
    if math.ceil((index+1)/2) != math.floor(no_variables/2):
        current_ax.set_xlabel('')
    current_ax.set_ylabel(formatted_names[variable])
    handles, labels = current_ax.get_legend_handles_labels()
    current_ax.get_legend().remove()
    index += 1

fig.legend(handles[1:], ['California', 'Namibia', 'Peru'], ncol=3, title='Location', loc='lower center')#, bbox_to_anchor=(0.525, 0.875))
fig.suptitle('Properties of POCs', fontsize=22)
if not os.path.exists('images'):
    os.makedirs('images')
plt.savefig('images/POC_properties_sd.png')
#plt.show()
