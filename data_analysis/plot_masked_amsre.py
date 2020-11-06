import xarray as xr
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pickle
import dask.array as da
import math
import matplotlib.pyplot as plt 
from glob import glob
import itertools
import os

fileload = 1
num_procs = 8
#dataDIR = 'Archive/*hdf_poc_5km.nc'
#dataDIR = '/gws/nopw/j04/eo_shared_data_vol2/scratch/dwatsonparris/AMSR2_*_????_lwp.nc'
#filelist = glob(dataDIR)
#print(len(filelist))
filelist = ['/gws/nopw/j04/eo_shared_data_vol2/scratch/dwatsonparris/AMSRE_89GHz_pcp_est_LWP_2003_206_day_MODIS_match_22_rain.nc']
print(filelist)


def datagen(n, filelist):
    for i in range(math.ceil(len(filelist)/n)):
        print('Batch', i)
        yield filelist[i*n:(i+1)*n]

def data_extract(file):
    try:
        DS = xr.open_mfdataset(file)
    except:
        print("That one didn't work! " + str(file))
        return None

    if not hasattr(DS, 'poc_mask'):
        print("file doesn't contain a mask: " + str(file))
        return None
        
    #if DS['poc_mask'].values.any() == 0:
    #    return None
    data = {}
    #data comes in as one long vector, reshape to modis image size, first dimension is image index
    #    rain_time = 263 ;
    #    rain_swath_width = 486 ;
    #    lwp_time = 263 ;
    #    lwp_swath_width = 243 ;

    #   rain_time = 264 ;
    #   rain_swath_width = 486 ;
    #   yr_day_utc = 3 ;
    #   lwp_time = 263 ;
    #   lwp_swath_width = 243 ;

    #print(DS.poc_mask.shape[0], shape)
    # AMSRE rain
    if DS.poc_mask.shape[0] % (265*486) == 0:
        shape = (int(DS.poc_mask.shape[0]/(265*486)),486,265)
    elif DS.poc_mask.shape[0] % (264*486) == 0:
        shape = (int(DS.poc_mask.shape[0]/(264*486)),486,264)
    elif DS.poc_mask.shape[0] % (263*486) == 0:
        shape = (int(DS.poc_mask.shape[0]/(263*486)),486,263)
    # AMSRE LWP
    elif DS.poc_mask.shape[0] % (265*243) == 0:
        shape = (int(DS.poc_mask.shape[0]/(265*243)),243,265)
    # AMSR2
#        rain_time = 528 ;
#        rain_swath_width = 486 ;
#        lwp_time = 264 ;
#        lwp_swath_width = 243 ;
    elif DS.poc_mask.shape[0] % (529*486) == 0:
        shape = (int(DS.poc_mask.shape[0]/(529*486)),486,529)
    elif DS.poc_mask.shape[0] % (528*486) == 0:
        shape = (int(DS.poc_mask.shape[0]/(528*486)),486,528)
    elif DS.poc_mask.shape[0] % (527*486) == 0:
        shape = (int(DS.poc_mask.shape[0]/(527*486)),486,527)
    elif DS.poc_mask.shape[0] % (264*243) == 0:
        shape = (int(DS.poc_mask.shape[0]/(264*243)),243,264)
    elif DS.poc_mask.shape[0] % (263*243) == 0:
        shape = (int(DS.poc_mask.shape[0]/(263*243)),243,263)
    elif DS.poc_mask.shape[0] % (265*243) == 0:
        shape = (int(DS.poc_mask.shape[0]/(265*243)),243,265)
    # TODO add 63909 shape...
#    elif DS.poc_mask.shape[0] % (265*243) == 0:
#        shape = (int(DS.poc_mask.shape[0]/(265*243)),265,243)
    else:
        print("Weird shape modis data: {}".format(DS.poc_mask.shape[0]))
        print(file)
        print(DS)
        return None

    print(shape)

    #extract numpy arrays into dict
    for variable in DS:
        if variable == 'time':
            continue
        #try: 
        data[variable] = DS[variable].values.reshape(shape)
        #except:
        #   print('Something went wrong in the reshaping!')
        #   return None

    #Discard images with no pocs
    poc_index = data['poc_mask'].any(axis=(1,2))
    poc_only_data = {}
    for variable, array in data.items():
        poc_only_data[variable] = array[poc_index,...]
        
    del(data, DS)
    poc_no = []
    
    graphs=[]
    for i in range(poc_only_data['poc_mask'].shape[0]):
        single_image_data={}
        for variable, array in poc_only_data.items():
            if variable == 'poc_mask':
                single_image_data[variable] = array[i,...]>127
            else:
                single_image_data[variable] = array[i,...]

            plt.imshow(single_image_data[variable])
            plt.show()
    return final_results
            
    
    
if __name__ == '__main__':
    inputs = [file for file in datagen(fileload,filelist)]
    output = list(map(data_extract, inputs))
    #print(output)
    pool.close()
    pool.join()

    if not os.path.exists('results'):
        os.makedirs('results')
    
    print('Saving...')
    with open('results/results_amsr2_rain.pickle', 'wb') as handle:
        pickle.dump(output, handle, protocol=pickle.HIGHEST_PROTOCOL)
