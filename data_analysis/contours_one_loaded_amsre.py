import xarray as xr
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pickle
import dask.array as da
import cv2
import math
from multiprocessing import Pool
from glob import glob
import itertools
import os

fileload = 1
num_procs = 8
#dataDIR = 'Archive/*hdf_poc_5km.nc'
#dataDIR = '/gws/nopw/j04/eo_shared_data_vol2/scratch/dwatsonparris/AMSR2_*rain.nc'
dataDIR = '/gws/nopw/j04/eo_shared_data_vol2/scratch/dwatsonparris/AMSRE_*_????_rain.nc'
filelist = glob(dataDIR)
print(len(filelist))
print(filelist)

def oneDArray(x):
    return list(itertools.chain(*x))

def image_data_operation(data, mask, method='Mean'):
    if method == 'Mean':
        return np.nanmean(data*mask)
    elif method == 'MinMax':
        return cv2.minMaxLoc(data.astype('float32'), mask = mask.astype('uint8'))
    
def striped_boundary_data(mask, stripe_width, no_stripes, data, method='Mean'):
    kernel = cv2.getStructuringElement(cv2.MORPH_ELLIPSE, (stripe_width*2, stripe_width*2))
    masks = np.zeros((no_stripes,mask.shape[0],mask.shape[1]))
    inner_mask = mask
    outer_mask = np.zeros((mask.shape[0],mask.shape[1]))
    #Create the inner stripes
    for i in range(int(math.floor(no_stripes/2))):
        inner_mask = cv2.erode(inner_mask.astype('float32'), kernel, iterations = 1)
        masks[math.floor(no_stripes/2-1)-i,...] = mask.astype('float32')-inner_mask-outer_mask
        outer_mask = outer_mask + masks[math.floor(no_stripes/2-1)-i,...]
    #Create the outer stripes
    outer_mask = mask.astype('float32')
    inner_mask = mask.astype('float32')
    for i in range(int(math.ceil(no_stripes/2))):
        outer_mask = cv2.dilate(outer_mask, kernel, iterations = 1)
        masks[math.floor(no_stripes/2)+i,...] = outer_mask - inner_mask
        inner_mask = outer_mask
    #Masks OK
    masks[masks==0] = np.nan

    graphs = {}
    for variable, array in data.items():
        graphs[variable] = np.zeros(no_stripes)
        #Do the data operation
        for i in range(masks.shape[0]):
            if np.amax(masks[i,...]) == 0:
                graphs[variable][i] = np.nan
            else:
                graphs[variable][i] = image_data_operation(array, masks[i,...], method=method)
    return graphs

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
    #if DS.poc_mask.shape[0] % (265*486)== 0:
    #    shape = (int(DS.poc_mask.shape[0]/(265*486)),486,265)
    #elif DS.poc_mask.shape[0] % (264*486) == 0:
    #    shape = (int(DS.poc_mask.shape[0]/(264*486)),486,264)
    #elif DS.poc_mask.shape[0] % (263*486) == 0:
    #    shape = (int(DS.poc_mask.shape[0]/(263*486)),486,263)
    # AMSRE LWP
    #elif DS.poc_mask.shape[0] % (265*243) == 0:
    #    shape = (int(DS.poc_mask.shape[0]/(265*243)),243,265)
    # AMSR2
#        rain_time = 528 ;
#        rain_swath_width = 486 ;
#        lwp_time = 264 ;
#        lwp_swath_width = 243 ;
    #elif DS.poc_mask.shape[0] % (529*486) == 0:
    #    shape = (int(DS.poc_mask.shape[0]/(529*486)),529,486)
    #elif DS.poc_mask.shape[0] % (528*486) == 0:
    #    shape = (int(DS.poc_mask.shape[0]/(528*486)),528,486)
    #elif DS.poc_mask.shape[0] % (527*486) == 0:
    #    shape = (int(DS.poc_mask.shape[0]/(527*486)),527,486)
    #elif DS.poc_mask.shape[0] % (264*243) == 0:
    #    shape = (int(DS.poc_mask.shape[0]/(264*243)),264,243)
    #elif DS.poc_mask.shape[0] % (263*243) == 0:
    #    shape = (int(DS.poc_mask.shape[0]/(263*243)),263,243)
    #elif DS.poc_mask.shape[0] % (265*243) == 0:
    #    shape = (int(DS.poc_mask.shape[0]/(265*243)),265,243)
    ## TODO add 63909 shape...
#    elif DS.poc_mask.shape[0] % (265*243) == 0:
#        shape = (int(DS.poc_mask.shape[0]/(265*243)),265,243)
    #else:
    #    print("Weird shape modis data: {}".format(DS.poc_mask.shape[0]))
    #    print(file)
    #    print(DS)
    #    return None

    #print(shape)

    #extract numpy arrays into dict
    for variable in DS:
        if variable == 'time':
            continue
        #try: 
        data[variable] = DS[variable].values #.reshape(shape)
        #except:
        #   print('Something went wrong in the reshaping!')
        #   return None

    #Discard images with no pocs
    poc_index = data['poc_mask'].any()
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
        label_no, labels = cv2.connectedComponents((poc_only_data['poc_mask'][i,:,:]>127).astype('uint8'))
        poc_no.append(label_no-1)
        for j in range(label_no-1):
            mask = (labels == j+1).astype('uint8')
            graphs.append(striped_boundary_data(mask, 2, 40, single_image_data))
    final_results = {}
    for variable, array in poc_only_data.items():
        final_results[variable] = []
    for poc in graphs:
        for variable, array in poc.items():
            final_results[variable].append(array)
            
    return final_results
            
    
    
if __name__ == '__main__':
    pool = Pool(num_procs)
    inputs = [file for file in datagen(fileload,filelist)]
    output = pool.map(data_extract, inputs)
    #print(output)
    pool.close()
    pool.join()

    if not os.path.exists('results'):
        os.makedirs('results')
    
    print('Saving...')
    with open('results/results_amsre_rain.pickle', 'wb') as handle:
        pickle.dump(output, handle, protocol=pickle.HIGHEST_PROTOCOL)
