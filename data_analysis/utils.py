import os
import pandas as pd
import numpy as np

original_width = 1320  #(a ratio of 0.5)
original_height = 1100  #(a ratio of 0.6)
regions = ['California', "Namibia", "Peru"]

lon = {'Californian':[-155,-95], 'Peruvian':[-115,-55], 'Namibian':[-25,35]}
lat = {'Californian':[5,55], 'Peruvian':[-45,5], 'Namibian':[-45,15]}
name_convert = {'Peru':'Peruvian', 'California':'Californian', 'Namibia':'Namibian'}


def assign_place(poc_df):
    res = []
    # We only need to test one of the slices of the POC
    single_slice_of_poc = poc_df.iloc[0]
    for place, value in lon.items():
        if single_slice_of_poc.latitude > lat[place][0] and single_slice_of_poc.latitude < lat[place][1] and \
          single_slice_of_poc.longitude >lon[place][0] and single_slice_of_poc.longitude < lon[place][1]:
            res.append(place)
    # Check we matched exactly one place
#     print(res)
#     assert len(res) == 1, 'summin went wrong ere!'
    if len(res) == 1:
        res = res[0]
    else:
        res = None
    return res

def get_combined_df(pickle_file, stripe_width, overwrite=False, cloudsat=True, AMSR=True):
    """
    stripe_width # Number of kilometers each stripe represents
    
    """
    import pickle
    import math
    
    csv_file = pickle_file + '.csv'
    if os.path.isfile(csv_file) and (not overwrite):
        return pd.read_csv(csv_file)
    else:
        with open(pickle_file, 'rb') as handle:
            data = pickle.load(handle)
            
    columns = ['latitude', 'longitude', 'Cloud_Effective_Radius', 'Cloud_Water_Path', 'Cloud_Optical_Thickness', 'poc_mask']  # MODIS
    
    if cloudsat:
        columns.extend(['cloudsat_rain_rate', 'cloudsat_rwp', 'cloudsat_lwp', 'cloudsat_cth1l', 'cloudsat_cbh1l'])          # Cloudsat
    
    if AMSR:
        columns.extend(['lwp', 'rain_rwr', 'rain_mean', 'rain_max', 'rain_prob'])                               # AMSR

    example_POC = next((d for d in data if (d is not None)))
    print(example_POC.keys())
    POCs = [len(d['latitude']) for d in data if d is not None]
    n_POCs = sum(POCs)
    print("Number of POCs found: {}".format(n_POCs))

    n_stripes = len(example_POC['latitude'][0])  # No. of stripes (assuming all the same)

    n_stripes_from_boundary = (math.ceil(n_stripes/2)-0.5)

    # Combine all the dictionaries into one with stacked arrays
    pandas_data = {}
    for key in columns:
        pandas_data[key] = np.zeros((0, n_stripes))

    for image in data:
        if image: 
            for variable, array in image.items():
                if variable in pandas_data:
                    pandas_data[variable] = np.append(pandas_data[variable], np.asarray(array), axis=0)
                    pandas_data[variable] = pd.DataFrame(pandas_data[variable])

    pandas_long_form = pd.DataFrame()
    for variable, array in pandas_data.items():
        pandas_long_form[variable] = array.stack()
        
    pandas_long_form['Distance from Boundary (km)'] = (pandas_long_form.index.get_level_values(1).values-n_stripes_from_boundary)*stripe_width
    
    # Group by POC, apply the assignment function, then reindex to broadcast across the slices
    pandas_long_form['Location'] = pandas_long_form.groupby(level=0, axis=0).apply(assign_place).reindex(pandas_long_form.index, level=0)

    pandas_long_form['POC'] = np.select([pandas_long_form['Distance from Boundary (km)'].values < -35.,
                                         pandas_long_form['Distance from Boundary (km)'].values > 35.,
                                         (-35. < pandas_long_form['Distance from Boundary (km)'].values) & (pandas_long_form['Distance from Boundary (km)'].values < 35.)], 
                                        ['Inside', 'Outside', 'Boundary'])
    
#     pandas_long_form['POC'] = np.select([pandas_long_form['Distance from Boundary (km)'].values < 0.,
#                                          pandas_long_form['Distance from Boundary (km)'].values > 0.],
#                                         ['Inside', 'Outside'])
    
#   # Save the df for next time
    pandas_long_form.to_csv(csv_file)
    return pandas_long_form

def load_poc_database(filename, overwrite=False):

    simple_poc_database = filename[:-4] + ".csv"
    
    if os.path.isfile(simple_poc_database) and (not overwrite):
        poc_data = pd.read_csv(simple_poc_database)
    else:
        from sklearn.externals import joblib
        from shapely.geometry import Polygon, MultiPolygon
        from shapely.affinity import scale
    #     poc_database = joblib.load('/Users/watson-parris/Local Data/POC_data/POC_database_2.dat')
        poc_database = joblib.load(filename)
        indices = []
        filename_index = 0
        filenames = {}
        filenames_list = []
        for filename, value in poc_database.items():
            filenames[filename] = filename_index
            filenames_list.append(filename)
            for poc in range(value['poc_no']):
                indices.append((filename_index,poc))
            filename_index += 1

        index = pd.MultiIndex.from_tuples(indices, names=['Image', 'POC'])
        columns = ['Poc Score', 'Area', 'Perimeter', 'Region']
        poc_data = pd.DataFrame(index=index, columns=columns)

        def get_region(f):

            for region in regions:
                if region.lower() in f:
                    return region
            print(f)
            raise ValueError("Unkown region")

        for filename, value in poc_database.items():
            for poc in range(value['poc_no']):
                poc_data.at[(filenames[filename], poc), 'Poc Score'] = value['poc_data'][poc]['poc_score']
                poc_data.at[(filenames[filename], poc), 'Area'] = value['poc_data'][poc]['area']
                poc_data.at[(filenames[filename], poc), 'Perimeter'] = value['poc_data'][poc]['perimeter']
                poc_data.at[(filenames[filename], poc), 'Region'] = get_region(filename)
                poc_data.at[(filenames[filename], poc), 'filename'] = filename
                poc_data.at[(filenames[filename], poc), 'date'] = pd.to_datetime(filename.split('.')[1][1:], format="%Y%j")
                # The original polygon
                poly = Polygon(np.squeeze(value['poc_data'][poc]['contour']))
                # Rescale to account fo image downsampling
                scaled_poly = scale(poly, xfact=original_width/648, yfact=original_height/648, origin=(0, 0))
                poc_data.at[(filenames[filename], poc), 'shape'] = scaled_poly
                poc_data.at[(filenames[filename], poc), 'shp_area'] = scaled_poly.area
        poc_data = poc_data.convert_dtypes()

        poc_data.to_csv(simple_poc_database)
        
    return poc_data