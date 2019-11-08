import iris
from glob import glob
import matplotlib.pyplot as plt
import matplotlib.patches as patch
import numpy as np
import cartopy.crs as ccrs

def load_callback(cube, field, fname):
    variable_attributes = ['date_metadata_modified', 'date_issued', 'date_modified', 'date_created',
                           'isccp_input_files', 'time_coverage_start', 'time_coverage_end', 'isccp_month', 'id',
                           'history']
    for a in variable_attributes:
        cube.attributes.pop(a)

#lon = {'cal':[230,240], 'per':[270,280], 'nam':[360,10]}
#lat = {'cal':[20,30], 'per':[-20,-10], 'nam':[-20,-10]}
lon = {'cal':[225,245], 'per':[265,285], 'nam':[355,15]}
lat = {'cal':[15,35], 'per':[-25,-5], 'nam':[-25,-5]}

time_series = {'cal':{}, 'nam':{}, 'per':{}}
for key, value in time_series.items():
    for i in range(1983,2016):
        time_series[key][i] = []
    
directory = '/Users/clim_proc/Documents/POC MPhys Project/Modis/ISCCP data/data/international-satellite-cloud-climate-project-isccp-h-series-data/access/isccp-basic/hgm/'
for filename in glob(directory+'ISCCP-Basic*'):
    
    year = int(filename.split('.')[4])
    c = (iris.load(filename, iris.Constraint(cube_func=lambda x:
    x.var_name=='cldamt'), callback=load_callback).concatenate_cube())
    
    for key, value in time_series.items():
        if key == 'nam':
            sub = c.extract(iris.Constraint(longitude=lambda cell: cell > lon[key][0] or cell < lon[key][1]) & #cal: 230 240, per: 270 280, nam: 0 10
                iris.Constraint(latitude=lambda cell: lat[key][0] < cell < lat[key][1]))
        else:
            sub = c.extract(iris.Constraint(longitude=lambda cell: lon[key][0] < cell < lon[key][1]) & #cal: 230 240, per: 270 280, nam: 0 10
                iris.Constraint(latitude=lambda cell: lat[key][0] < cell < lat[key][1])) #cal: 20 30, per: -20 -10, nam: -20 -10

        time_series[key][year].append(float(sub.collapsed(['latitude', 'longitude'], iris.analysis.MEAN, weights=iris.analysis.cartography.area_weights(sub)).data))

total = {'cal':[0]*12, 'per':[0]*12, 'nam':[0]*12}
count = {'cal':[0]*12, 'per':[0]*12, 'nam':[0]*12}
mean = {'cal':[0]*12, 'per':[0]*12, 'nam':[0]*12}

for place, years in time_series.items():
    for year, data in years.items():
        for i in range(len(data)):
            total[place][i] += data[i]
            count[place][i] += 1
            
for place, data in mean.items():
    for i in range(len(data)):
        mean[place][i] = total[place][i]/count[place][i]
        
months=['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
for place, _ in time_series.items():
    f = plt.figure()
    for year, value in time_series[place].items():
        plt.plot(value)
    plt.plot(mean[place], linewidth=4)
    plt.title(place)
    plt.xlabel('Month')
    plt.ylabel('Cloud amount')
    plt.xticks(np.arange(12), months)
    plt.show()
    
ax = plt.axes(projection=ccrs.PlateCarree())
ax.stock_img()
for place, coords in lat.items():
    if place == 'nam':
        rect = patch.Rectangle((lon[place][0],lat[place][0]), 360-lon[place][0]+lon[place][1], lat[place][1]-lat[place][0], transform=ccrs.PlateCarree(), linewidth=1, edgecolor='r', facecolor='none')
        ax.add_patch(rect)
    else:
        rect = patch.Rectangle((lon[place][0],lat[place][0]), lon[place][1]-lon[place][0], lat[place][1]-lat[place][0], transform=ccrs.PlateCarree(), linewidth=1, edgecolor='r', facecolor='none')
        ax.add_patch(rect)
plt.show()