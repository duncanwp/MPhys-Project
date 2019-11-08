import cis
from cis.data_io.gridded_data import make_from_cube
from glob import glob
from multiprocessing import Pool
import math
import copy

num_procs = 12
num_loaded = 30
dataDIR = 'Archive/*hdf_poc_1km.nc'
filelist = glob(dataDIR)
print(len(filelist))

def datagen(n, filelist):
    for i in range(math.ceil(len(filelist)/n)):
        yield filelist[i*n:(i+1)*n]

def density(file):
    d = cis.read_data(file, 'poc_mask')
    binned = d.aggregate(longitude=[-180,180,1], latitude=[-90,90,1])
    binned.append(d.aggregate(longitude=[-180,180,1], latitude=[-90,90,1], how='sum'))
    for info in binned:
        info.data = info.data.filled(0.0)
    density = make_from_cube(binned[3]/binned[2])
    count = binned[2]
    sums = binned[3]
    density.save_data('./density_data/'+file[0][-38:-16]+'_density_1km.nc')
    count.save_data('./density_data/'+file[0][-38:-16]+'_count_1km.nc')
    sums.save_data('./density_data/'+file[0][-38:-16]+'_sum_1km.nc')
    return None

if __name__ == '__main__':
    if not os.path.exists('density_data'):
        os.makedirs('density_data')
    pool = Pool(num_procs)
    data = [file for file in datagen(num_loaded,filelist)]
    results = pool.map(density, data)
    pool.close()
    pool.join()
    
  #  d = cis.read_data(dataDIR, 'poc_mask')

  #  a_mean, a_std, a_count = d.aggregate(longitude=[-180,180,1], latitude=[-90,90,1])
  #  a_sum = d.aggregate(longitude=[-180,180,1], latitude=[-90,90,1], how='sum')

   # fig = plt.figure(figsize=(20,20))

   # a_sum.plot()
   # plt.savefig('test.png')

   # b=make_from_cube(a_sum/a_count)
   # b.plot()