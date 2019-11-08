import iris
from glob import glob

def load_callback(cube, field, fname):
    variable_attributes = ['date_metadata_modified', 'date_issued', 'date_modified', 'date_created',
                           'isccp_input_files', 'time_coverage_start', 'time_coverage_end', 'isccp_month', 'id',
                           'history']
    for a in variable_attributes:
        cube.attributes.pop(a)

directory = '/Users/clim_proc/Documents/POC MPhys Project/Modis/ISCCP data/data/international-satellite-cloud-climate-project-isccp-h-series-data/access/isccp-basic/hgm/'
for filename in glob(directory+'ISCCP-Basic*'):

    c = iris.load(filename, iris.Constraint(cube_func=lambda x:
    x.var_name=='cldamt'), callback=load_callback).concatenate_cube()


    print(c)

    sub = c.extract(iris.Constraint(longitude=lambda cell: 230 < cell < 240) & #cal: 230 240, per: 270 280, nam: 0 10
                iris.Constraint(latitude=lambda cell: 20 < cell < 30)) #cal: 20 30, per: -20 -10, nam: -20 -10

    print(sub)

    time_series = sub.collapsed(['latitude', 'longitude'], iris.analysis.MEAN, weights=iris.analysis.cartography.area_weights(sub))

    print(time_series.data)
