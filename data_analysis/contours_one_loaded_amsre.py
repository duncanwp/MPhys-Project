import xarray as xr
import numpy as np
import pickle
import cv2
from multiprocessing import Pool
from glob import glob
import itertools
import os


def image_data_operation(data, mask, method='Mean'):
    if method == 'Mean':
        return np.nanmean(data*mask)
    elif method == 'MinMax':
        return cv2.minMaxLoc(data.astype('float32'), mask=mask.astype('uint8'))


def striped_boundary_data(mask, stripe_width, no_stripes, data, method='Mean'):
    kernel = cv2.getStructuringElement(cv2.MORPH_ELLIPSE, (stripe_width*2, stripe_width*2))
    masks = np.zeros((no_stripes, mask.shape[0], mask.shape[1]))
    inner_mask = mask
    outer_mask = np.zeros((mask.shape[0], mask.shape[1]))
    #Create the inner stripes
    for i in range(int(math.floor(no_stripes/2))):
        inner_mask = cv2.erode(inner_mask.astype('float32'), kernel, iterations=1)
        masks[math.floor(no_stripes/2-1)-i, ...] = mask.astype('float32')-inner_mask-outer_mask
        outer_mask = outer_mask + masks[math.floor(no_stripes/2-1)-i, ...]
    #Create the outer stripes
    outer_mask = mask.astype('float32')
    inner_mask = mask.astype('float32')
    for i in range(int(math.ceil(no_stripes/2))):
        outer_mask = cv2.dilate(outer_mask, kernel, iterations=1)
        masks[math.floor(no_stripes/2)+i, ...] = outer_mask - inner_mask
        inner_mask = outer_mask
    #Masks OK
    masks[masks == 0] = np.nan

    graphs = {}
    for variable, array in data.items():
        graphs[variable] = np.zeros(no_stripes)
        #Do the data operation
        for i in range(masks.shape[0]):
            if np.amax(masks[i,...]) == 0:
                graphs[variable][i] = np.nan
            else:
                graphs[variable][i] = image_data_operation(array, masks[i, ...], method=method)
    return graphs


def data_extract(file, stripe_width=1, n_stripes=40):
    try:
        DS = xr.open_mfdataset(file)
    except:
        print("That one didn't work! " + str(file))
        return None

    if not hasattr(DS, 'poc_mask'):
        print("file doesn't contain a mask: " + str(file))
        return None

    label_no, labels = cv2.connectedComponents((DS['poc_mask']>127).astype('uint8'))

    graphs = []
    for j in range(label_no-1):
        mask = (labels == j+1).astype('uint8')
        # Process all dataset variables (including coordinates)
        graphs.append(striped_boundary_data(mask, stripe_width, n_stripes, DS.variabes))

    # Currently graph is a list of dicts. This turns that into a dict of lists...
    final_results = {}
    for variable, array in poc_only_data.items():
        final_results[variable] = []
    for poc in graphs:
        for variable, array in poc.items():
            final_results[variable].append(array)
            
    return final_results

    
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('files', nargs='*', help="Input file(s)")
    parser.add_argument('-o', '--outfile', help="Output name", default='poc')
    parser.add_argument('--overwrite', help="Output name", action='store_true')
    parser.add_argument('-n', '--processes', help="Number of processes to run", default=1, type=int)
    parser.add_argument('--stripes', help="Number of stripes around each POC", default=40, type=int)
    parser.add_argument('--stripe-width', help="Pixel width of stripe", default=1, type=int)

    # Gets command line args by default
    args = parser.parse_args()
    outfile = f"results_{args.outfile}.pickle"
    if os.path.isfile(outfile) and not args.overwrite:
        print(f"Output file: {outfile} already exists. Exiting")
    else:
        # Read each file across as many processes as we can
        with Pool(args.processes) as p:
            output = p.map(partial(data_extract,
                                   stripe_width=args.stripe_width,
                                   n_stripes=args.stripes),
                           args.files)

        print(f'Saving output file: {outfile}')
        with open(outfile, 'wb') as f:
            pickle.dump(output, f, protocol=pickle.HIGHEST_PROTOCOL)
