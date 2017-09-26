# pylint: disable=W0311

import netCDF4 as nc
import numpy as np
import requests
import random

def get_index(value, variable):
    return np.argmin(abs(variable - value))


def get_wcps_index(query):
    payload = {'query': query}
    r = requests.post(
        'http://earthserver.pml.ac.uk/rasdaman/ows/wcps', data=payload)
    return r.text[1:-1].split(':')[0]

def gen_random_lat_lon_int():
    return (random.randint(-89,89),random.randint(-179,179))

def gen_random_lat_lon_float():
    return (random.uniform(-89,89),random.uniform(-179,179))



test_lat_lons = [
    (36.9199981689, -8.61900043488),
    (30.8374004364, -80.7643966675),
    (42.8441009521, -19.9591007233),
    (41.7244987488, -20.0016002655),
    (36.92, -8.619),
    (30.8374, -80.7644),
    (42.8441, -19.9591),
    (42.8446, -19.9598),
    (42.45, -19.9672),
    (42.417, -19.961),
    (42.0343, -19.9993),
    (41.7245, -20.0016),
    (41.3224, -20.0069),
    (32.912, -117.393),
    (40.9291, -20.002),
    (37.1231, -19.8676),
    (36.8718, -19.8187),
    (35.0356, -19.1422),
    (32.9284, -79.2897),
    (29.847, -123.585),
    (31.7826, -17.8372),
    (32.43, -119.962),
    (32.43, -119.962),
    (31.475, -18.2116),
    (31.1666, -18.606),
    (30.8372, -18.9853),
    (30.5009, -19.3523),
    (30.1564, -19.71),
    (22.47, -158.13),
    (20.7928, -157.207),
    (28.6364, -20.9943),
    (28.1127, -20.9965),
    (27.6849, -20.9861),
    (33.422, -117.91),
    (27.1222, -20.9926),
    (20.7928, -157.207),
    (25.4952, -20.9987),
    (25.0299, -20.9904),
    (24.5694, -20.9922),
    (24.1392, -20.9995),
    (23.234, -21.0042),
    (33.325, -119.667),
    (33.153, -120.01),
    (21.2383, -20.8511),
    (20.8254, -20.7646),
    (31.992, -122.407),
    (16.6999, -19.9994),
    (8.8233, -21.7333),
    (5.0392, -23.2611),
    (34.332, -120.815),
    (20.7368, -157.002),
    (34.157, -121.157),
    (0.5418, -25.106),
    (-0.1632, -25.4073),
    (-0.5136, -25.5595),
    (-0.7727, -25.6632),
    (-0.7726, -25.6631),
    (33.493, -122.525),
    (33.152, -123.225),
    (-2.7867, -27.3682),
    (20.8583, -156.747),
    (5.99, -124.93),
    (-7.9446, -28.6641),
    (5.993, -124.938),
    (-10.8006, -29.8718),
    (-11.1604, -30.0405),
    (5.17, -124.832),
    (31.654, -64.126),
    (31.653, -64.116),
    (-19.3274, -33.4837),
    (42.95, 35.6),
    (-20.4182, -34.0616),
    (-21.0817, -34.556),
    (-21.8544, -35.2803),
    (41.8125, -68.3922),
    (-25.8142, -39.1074),
    (-26.0654, -39.3668),
    (-3.99, -124.82),
    (-26.3987, -39.7221),
    (-26.737, -40.0711),
    (-3.991, -124.822),
    (-30.614, -43.8666),
    (39.29, 25.11),
    (42.588, -67.5148),
    (59.69, -149.328),
    (-32.25, -49.33),
    (-32.25, -49.33),
    (27.659, -111.228),
    (-32.09, -49.67),
    (-32.09, -49.67),
    (17.6, -67.0),
    (17.3, -67.3),
    (27.844, -111.042),
    (26.982, -110.97),
    (34.16, -119.952),
    (34.087, -120.029),
    (34.344, -119.86),
    (34.39, -119.84),
    (34.39, -119.84),
    (34.3, -119.887),
    (34.3, -119.887),
    (34.261, -119.901),
    (34.261, -119.901),
    (34.2035, -119.928)
]


test_lat_lons_small = [
    (36.9199981689, -8.61900043488),
    (30.8374004364, -80.7643966675),
    (42.8441009521, -19.9591007233),
    (41.7244987488, -20.0016002655),
    (36.92, -8.619),
]

lat_query = '''
for a in (OC_CCI_V3_chlor_a_daily_uncert_combined_nc_tiling2)

return 

imageCrsDomain(a[Lat({Lat}:{Lat}), Long({Long}), ansi("2002-01-01")], Lat)
'''

lon_query = '''
for a in (OC_CCI_V3_chlor_a_daily_uncert_combined_nc_tiling2)

return 

imageCrsDomain(a[Lat({Lat}), Long({Long}:{Long}), ansi("2002-01-01")], Long)
'''


netcdf_file = '/data/datasets/CCI/v3.0-release/geographic/netcdf/daily/chlor_a/2000/ESACCI-OC-L3S-CHLOR_A-MERGED-1D_DAILY_4km_GEO_PML_OCx-20001209-fv3.0.nc'

dataset = nc.Dataset(netcdf_file)

lat_variable = dataset.variables['lat'][:]
lon_variable = dataset.variables['lon'][:]

lat_dif_count = 0
lon_dif_count = 0

run_count = 400
gen_lls = []

# for p in range(run_count):
#     gen_lls.append(gen_random_lat_lon_float())
    


# for a in range(5):
#     lat_dif_count = 0
#     lon_dif_count = 0
#     for x in gen_lls:
#         #x = gen_random_lat_lon()
#         nc_lat = get_index(x[0], lat_variable)
#         nc_lon = get_index(x[1], lon_variable)
#         lat_q = lat_query.format(Lat=str(float(x[0])), Long=str(float(x[1])))
#         lon_q = lon_query.format(Lat=str(float(x[0])), Long=str(float(x[1])))
#         print lat_q
#         print lon_q        
#         wcps_lat = get_wcps_index(lat_q)
#         wcps_lon = get_wcps_index(lon_q)
#         lat_diff = nc_lat - int(wcps_lat)
#         lon_diff = nc_lon - int(wcps_lon)
#         #print "({})testing {},{}".format(y,x[0],x[1])
#         #print "testing after convert to float {},{}".format(float(x[0]),float(x[1]))
#         if lat_diff != 0:
#             lat_dif_count = lat_dif_count + 1
#             lon_diff = nc_lon - int(wcps_lon)
#         if lon_diff != 0:
#             lon_dif_count = lon_dif_count + 1
#         if lon_diff != 0 or lat_diff != 0:
#             pass
#         print "testing values (lat, lon) : " + str(float(x[0])) +  str(float(x[1]))
#         print "Lat index = " + str(nc_lat)
#         print "Lon index = " + str(nc_lon)
#         print "Lat value from WCPS = " + wcps_lat
#         print "Lat value from WCPS = " + wcps_lon
#         print "difference for lat =" + str(lat_diff)
#         print "difference for lon =" + str(lon_diff)
#         print '-' * 20

#     print '-'*30
#     print "latitude errors {}/400".format(lat_dif_count)
#     print "longitude errors {}/400".format(lon_dif_count)

# dataset.close()
lat= 6.44
lon = 5.44
lat_q = lat_query.format(Lat=str(lat), Long=str(lon))
lon_q = lon_query.format(Lat=str(lat), Long=str(lon))

nc_lat = get_index(lat, lat_variable)
nc_lon = get_index(lon, lon_variable)

print nc_lat
print nc_lon

print lat_q
print lon_q

wcps_lat = get_wcps_index(lat_q)
wcps_lon = get_wcps_index(lon_q)
print wcps_lat
print wcps_lon