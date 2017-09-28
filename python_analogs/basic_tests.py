import netCDF4 as nc
import numpy as np
import glob
#import xarray as xr
import timeit
import requests

CCI_V3_DAILY_PATH='/data/datasets/CCI/v3.0-release/geographic/netcdf/daily/chlor_a/' # /years/365_files
CCI_V3_MONTHLY_PATH='/data/datasets/CCI/v3.0-release/geographic/netcdf/monthly/chlor_a' # /years/12_files
lat_bnds = [70, 0]
lon_bnds =  [-20, 10]


def get_index(value, variable):
    return np.argmin(abs(variable - value))


def copy_nc_dims_attr(dsin, dsout,lat_length=None,lon_length=None):
   for dname, the_dim in dsin.dimensions.iteritems():
      print dname, len(the_dim)
      if(lat_length and dname=='lat'):
          dsout.createDimension(dname, lat_length+1 )
      elif(lon_length and dname=='lon'):
          dsout.createDimension(dname, lon_length+1 )
      else:
          dsout.createDimension(dname, len(the_dim) if not the_dim.isunlimited() else None)
   for name in dsin.ncattrs():
      dsout.setncattr(name, dsin.getncattr(name))

def get_comparison_of_chl_to_rmsd_year_dailies():
    pass

def get_comparison_of_chl_to_rmsd_year_dailies_rasdaman():
    pass

def get_count_valid_pixels_year_dailies_xarray():
   # find all files that are needed
   # loop each and open
   # extract number of valid pixels
   # close file
   # create 'csv' to match ras format
   # end
   count = 0
   output = []
   #print "starting xarray test"
   #for f in sorted(glob.glob(CCI_V3_DAILY_PATH+'/2002/*.nc')): #for future use the pixel cords for this year are 1553-1917
      #print "opening {file} for testing.....".format(file=f)
   ncfile  = xr.open_mfdataset(CCI_V3_DAILY_PATH+'/2002/*.nc')
   count = count + 1
   #print ncfile['chlor_a'].count().values
   output.append(int(ncfile['chlor_a'].count().values))
   #if count>=365:
   #   break
   #print output


def get_count_valid_pixels_year_dailies():
   # find all files that are needed
   # loop each and open
   # extract number of valid pixels
   # close file
   # create 'csv' to match ras format
   # end
   count = 0
   output = []
   #print "starting normal numpy test"
   for f in sorted(glob.glob(CCI_V3_DAILY_PATH+'/2002/*.nc')): #for future use the pixel cords for this year are 1553-1917
      #print "opening {file} for testing.....".format(file=f)
      ncfile = nc.Dataset(f)
      chlor_a = ncfile.variables['chlor_a'][:]

      #print chlor_a.shape
      #print chlor_a.count()
      ncfile.close()
      count = count + 1
      output.append(chlor_a.count())
      if count>=365:
         break
   print output


def get_count_valid_pixels_year_dailies_geo_subset():

   Lats = (46.44,48.44)
   Longs = (-40.44,-38.44)
   # find all files that are needed
   # loop each and open
   # extract number of valid pixels
   # close file
   # create 'csv' to match ras format
   # end
   count = 0
   output = []
   #print "starting normal numpy test"
   for f in sorted(glob.glob(CCI_V3_DAILY_PATH+'/2002/*.nc')): #for future use the pixel cords for this year are 1553-1917
      #print "opening {file} for testing.....".format(file=f)
      ncfile = nc.Dataset(f)
      lats = ncfile.variables['lat'][:]
      lons = ncfile.variables['lon'][:]
      lat_idxs = [get_index(lats,Lats[0]),get_index(lats,Lats[1])]
      lon_idxs = [get_index(lons,Longs[0]),get_index(lons,Longs[1])]
      #print lat_idxs
      #print lon_idxs
      chlor_a = ncfile.variables['chlor_a'][0,lat_idxs[1]:lat_idxs[0]+1,lon_idxs[0]:lon_idxs[1]+1]
      #chlor_a = np.ma.MaskedArray(chlor_a)
      #print chlor_a
      #print chlor_a.count()
      #print np.nanmax(chlor_a)
      count = count + 1
      #output.append(chlor_a.size)
      if(chlor_a.all() == None):
          output.append(0)
      else:
           try:
              output.append(chlor_a.count())
           except:
              output.append(chlor_a.size)
      ncfile.close()
      if count>=365:
         break
   print output


def get_single_point_timeseries_year_of_dailies():
   lat= 46.44
   lon = -44.44
   # find all files that are needed
   # loop each and open
   # extract number of valid pixels
   # close file
   # create 'csv' to match ras format
   # end
   count = 0
   output = []
   #print "starting normal numpy test"
   for f in sorted(glob.glob(CCI_V3_DAILY_PATH+'/2002/*.nc')): #for future use the pixel cords for this year are 1553-1917
      #print "opening {file} for testing.....".format(file=f)
      ncfile = nc.Dataset(f)
      lats = ncfile.variables['lat'][:]
      lons = ncfile.variables['lon'][:]
      lat_idxs = get_index(lats,lat)
      lon_idxs = get_index(lons,lon)
      #print lat_idxs
      #print lon_idxs
      chlor_a = ncfile.variables['chlor_a'][0,lat_idxs,lon_idxs]
      chl_val = chlor_a.tolist()
      #print chlor_a[:]
      #print chlor_a.count()
      #print np.nanmax(chlor_a)
      count = count + 1
      if(chl_val == None):
          output.append(0.0)
      else:
          output.append(chlor_a.tolist())
      ncfile.close()

      if count>=365:
         break
   print output

def get_single_point_timeseries_year_of_dailies_rasdaman():
    query = """for c in (OC_CCI_V3_chlor_a_daily_final_test)
return
encode (
c[Lat(46.44),Long(-5.44),ansi("2002-01-01T00:00:00.000Z":"2002-12-31T00:00:00.000Z")] *
(c[Lat(46.44),Long(-5.44),ansi("2002-01-01T00:00:00.000Z":"2002-12-31T00:00:00.000Z")] < 10000))
, "csv")
"""
    
    resp = requests.post('http://aurora.npm.ac.uk:8080/rasdaman/ows/wcps', data = {'query':query})
    data = resp.text
    #"{72711,209486,553488,414124,717472,1.38544e+06,546184,1.92304e+06,2.38247e+06,2.26598e+06,2.51428e+06,2.39622e+06,2.47347e+06,2.4621e+06,2.41684e+06,2.34636e+06,2.29236e+06,2.27524e+06,2.14153e+06,2.13028e+06,2.07111e+06,2.19592e+06,2.07689e+06,2.22208e+06,2.28352e+06,2.21018e+06,2.4767e+06,2.09365e+06,2.32056e+06,2.05412e+06,2.30236e+06,465816,2.15678e+06,2.25427e+06,1.41406e+06,1.24801e+06,1.80251e+06,2.15251e+06,2.18445e+06,2.14107e+06,2.11503e+06,1.91002e+06,2.16944e+06,2.12247e+06,2.17722e+06,2.21804e+06,2.34782e+06,2.1115e+06,2.12382e+06,2.11024e+06,2.32768e+06,1.94016e+06,2.19825e+06,2.45855e+06,2.33082e+06,2.55823e+06,2.26572e+06,2.46334e+06,2.18902e+06,2.31511e+06,2.15154e+06,2.21785e+06,2.13276e+06,2.30657e+06,2.14245e+06,2.19113e+06,2.06309e+06,1.60451e+06,2.20502e+06,2.18906e+06,2.24779e+06,2.25791e+06,2.3423e+06,2.28886e+06,2.28319e+06,2.43677e+06,2.13043e+06,2.24797e+06,2.18346e+06,2.17671e+06,2.14745e+06,2.29615e+06,2.152e+06,2.2513e+06,2.0865e+06,2.16888e+06,2.24366e+06,1.83247e+06,2.37524e+06,2.29223e+06,2.24434e+06,2.03229e+06,2.12637e+06,2.37571e+06,2.16999e+06,2.37027e+06,2.34807e+06,2.67477e+06,2.293e+06,2.12044e+06,2.14532e+06,2.03663e+06,2.06073e+06,2.062e+06,2.1806e+06,2.39518e+06,2.50413e+06,2.22647e+06,2.39676e+06,2.31182e+06,2.37906e+06,2.43791e+06,2.30384e+06,2.46389e+06,2.42558e+06,2.402e+06,2.45935e+06,2.28188e+06,1.92898e+06,2.14165e+06,2.19624e+06,2.18433e+06,2.33889e+06,2.15939e+06,2.38333e+06,2.26662e+06,2.25718e+06,2.23495e+06,2.45611e+06,2.30751e+06,2.41481e+06,2.46667e+06,2.43233e+06,2.63702e+06,2.21819e+06,2.44397e+06,2.51633e+06,2.65564e+06,2.3125e+06,2.65752e+06,2.53365e+06,2.61287e+06,2.59192e+06,2.70776e+06,2.39989e+06,2.3448e+06,2.49624e+06,2.42241e+06,2.5633e+06,2.46257e+06,2.45369e+06,2.39753e+06,2.53399e+06,2.4725e+06,2.52749e+06,2.67958e+06,2.56465e+06,2.45879e+06,2.51158e+06,2.41193e+06,2.52716e+06,2.68409e+06,2.62837e+06,2.80027e+06,2.53803e+06,2.4316e+06,2.64254e+06,2.70866e+06,2.74679e+06,2.72809e+06,2.61876e+06,2.2369e+06,2.34587e+06,2.22522e+06,2.37346e+06,2.39164e+06,2.39016e+06,2.54166e+06,2.49588e+06,2.46917e+06,2.4755e+06,2.42946e+06,2.33502e+06,2.33711e+06,2.34948e+06,2.21379e+06,2.34958e+06,2.34579e+06,2.40145e+06,2.42961e+06,2.6829e+06,2.5989e+06,2.51022e+06,2.35346e+06,2.58052e+06,2.5865e+06,2.54789e+06,2.54811e+06,2.53794e+06,2.73524e+06,2.37201e+06,2.34987e+06,2.35207e+06,2.49352e+06,2.31479e+06,2.31441e+06,2.3499e+06,2.37542e+06,2.47956e+06,2.29096e+06,2.18724e+06,2.2568e+06,2.31909e+06,2.25994e+06,2.23682e+06,2.24437e+06,2.35153e+06,2.31288e+06,2.54672e+06,2.51219e+06,2.36942e+06,2.35156e+06,2.36418e+06,2.36654e+06,2.38407e+06,2.34993e+06,2.14708e+06,2.11976e+06,2.32577e+06,2.28716e+06,2.33583e+06,2.19651e+06,2.24871e+06,2.15737e+06,2.35147e+06,2.26302e+06,2.19812e+06,2.0875e+06,2.03056e+06,1.96203e+06,2.06018e+06,2.11128e+06,2.10853e+06,2.13374e+06,2.10901e+06,2.26296e+06,2.0164e+06,2.09115e+06,2.03792e+06,2.12637e+06,2.14085e+06,2.16001e+06,2.2756e+06,2.24114e+06,2.41336e+06,2.2634e+06,2.34329e+06,2.2264e+06,2.33302e+06,2.24152e+06,1.9862e+06,2.15778e+06,2.2829e+06,2.23893e+06,2.11565e+06,2.30374e+06,2.30916e+06,2.5187e+06,2.43082e+06,2.55497e+06,2.46965e+06,2.27593e+06,2.17774e+06,2.12589e+06,2.26873e+06,2.03262e+06,2.2214e+06,2.12517e+06,2.04509e+06,2.08353e+06,2.09286e+06,2.16079e+06,2.33772e+06,2.02613e+06,1.79022e+06,2.08997e+06,1.96038e+06,2.10368e+06,1.86843e+06,2.35958e+06,2.30029e+06,2.51754e+06,2.20115e+06,1.67929e+06,2.36588e+06,2.47476e+06,2.4662e+06,2.48749e+06,2.4781e+06,2.6275e+06,2.55799e+06,2.65382e+06,2.43487e+06,2.49468e+06,2.816e+06,2.75566e+06,2.6963e+06,2.46098e+06,2.37014e+06,2.29009e+06,2.52903e+06,2.29543e+06,2.18464e+06,2.13366e+06,2.41024e+06,2.50854e+06,2.50553e+06,2.76764e+06,2.37775e+06,2.52245e+06,2.71848e+06,2.75275e+06,2.52737e+06,2.63753e+06,2.55228e+06,2.52562e+06,2.51038e+06,2.71144e+06,2.63684e+06,2.85916e+06,2.90703e+06,2.71774e+06,2.82557e+06,2.62867e+06,2.85164e+06,2.73356e+06,2.82692e+06,2.77557e+06,2.74124e+06,2.85076e+06,2.63225e+06,2.75354e+06,2.509e+06,2.71797e+06,2.63662e+06,2.69564e+06,2.91593e+06,2.93727e+06,3.01304e+06,2.8006e+06,2.67717e+06,2.74294e+06,3.05328e+06,2.9094e+06,2.80031e+06,2.62486e+06,2.57921e+06,2.71855e+06,2.67038e+06,2.8567e+06,2.82475e+06,3.10879e+06,2.86793e+06,2.91522e+06,2.75267e+06}"
    #data = data[1:-1]
    #print data
    data = [float(x) for x in data.split(',')]
    print data


def get_count_valid_pixel_year_dailies_rasdaman():
    # get query from txt file
    # make http request
    # parse response
    query = """for c in  (OC_CCI_V3_chlor_a_daily_final_test)
return 
encode (

coverage histogram over

$pansi ansi(imageCrsDomain(c[ansi("2002-01-01T00:00:00.000Z":"2002-12-31T00:00:00.000Z")], ansi))

values (

count(c[ansi($pansi)] < 1000000)
)
, "csv")"""
    
    resp = requests.post('http://aurora.npm.ac.uk:8080/rasdaman/ows/wcps', data = {'query':query})
    data = resp.text
    #"{72711,209486,553488,414124,717472,1.38544e+06,546184,1.92304e+06,2.38247e+06,2.26598e+06,2.51428e+06,2.39622e+06,2.47347e+06,2.4621e+06,2.41684e+06,2.34636e+06,2.29236e+06,2.27524e+06,2.14153e+06,2.13028e+06,2.07111e+06,2.19592e+06,2.07689e+06,2.22208e+06,2.28352e+06,2.21018e+06,2.4767e+06,2.09365e+06,2.32056e+06,2.05412e+06,2.30236e+06,465816,2.15678e+06,2.25427e+06,1.41406e+06,1.24801e+06,1.80251e+06,2.15251e+06,2.18445e+06,2.14107e+06,2.11503e+06,1.91002e+06,2.16944e+06,2.12247e+06,2.17722e+06,2.21804e+06,2.34782e+06,2.1115e+06,2.12382e+06,2.11024e+06,2.32768e+06,1.94016e+06,2.19825e+06,2.45855e+06,2.33082e+06,2.55823e+06,2.26572e+06,2.46334e+06,2.18902e+06,2.31511e+06,2.15154e+06,2.21785e+06,2.13276e+06,2.30657e+06,2.14245e+06,2.19113e+06,2.06309e+06,1.60451e+06,2.20502e+06,2.18906e+06,2.24779e+06,2.25791e+06,2.3423e+06,2.28886e+06,2.28319e+06,2.43677e+06,2.13043e+06,2.24797e+06,2.18346e+06,2.17671e+06,2.14745e+06,2.29615e+06,2.152e+06,2.2513e+06,2.0865e+06,2.16888e+06,2.24366e+06,1.83247e+06,2.37524e+06,2.29223e+06,2.24434e+06,2.03229e+06,2.12637e+06,2.37571e+06,2.16999e+06,2.37027e+06,2.34807e+06,2.67477e+06,2.293e+06,2.12044e+06,2.14532e+06,2.03663e+06,2.06073e+06,2.062e+06,2.1806e+06,2.39518e+06,2.50413e+06,2.22647e+06,2.39676e+06,2.31182e+06,2.37906e+06,2.43791e+06,2.30384e+06,2.46389e+06,2.42558e+06,2.402e+06,2.45935e+06,2.28188e+06,1.92898e+06,2.14165e+06,2.19624e+06,2.18433e+06,2.33889e+06,2.15939e+06,2.38333e+06,2.26662e+06,2.25718e+06,2.23495e+06,2.45611e+06,2.30751e+06,2.41481e+06,2.46667e+06,2.43233e+06,2.63702e+06,2.21819e+06,2.44397e+06,2.51633e+06,2.65564e+06,2.3125e+06,2.65752e+06,2.53365e+06,2.61287e+06,2.59192e+06,2.70776e+06,2.39989e+06,2.3448e+06,2.49624e+06,2.42241e+06,2.5633e+06,2.46257e+06,2.45369e+06,2.39753e+06,2.53399e+06,2.4725e+06,2.52749e+06,2.67958e+06,2.56465e+06,2.45879e+06,2.51158e+06,2.41193e+06,2.52716e+06,2.68409e+06,2.62837e+06,2.80027e+06,2.53803e+06,2.4316e+06,2.64254e+06,2.70866e+06,2.74679e+06,2.72809e+06,2.61876e+06,2.2369e+06,2.34587e+06,2.22522e+06,2.37346e+06,2.39164e+06,2.39016e+06,2.54166e+06,2.49588e+06,2.46917e+06,2.4755e+06,2.42946e+06,2.33502e+06,2.33711e+06,2.34948e+06,2.21379e+06,2.34958e+06,2.34579e+06,2.40145e+06,2.42961e+06,2.6829e+06,2.5989e+06,2.51022e+06,2.35346e+06,2.58052e+06,2.5865e+06,2.54789e+06,2.54811e+06,2.53794e+06,2.73524e+06,2.37201e+06,2.34987e+06,2.35207e+06,2.49352e+06,2.31479e+06,2.31441e+06,2.3499e+06,2.37542e+06,2.47956e+06,2.29096e+06,2.18724e+06,2.2568e+06,2.31909e+06,2.25994e+06,2.23682e+06,2.24437e+06,2.35153e+06,2.31288e+06,2.54672e+06,2.51219e+06,2.36942e+06,2.35156e+06,2.36418e+06,2.36654e+06,2.38407e+06,2.34993e+06,2.14708e+06,2.11976e+06,2.32577e+06,2.28716e+06,2.33583e+06,2.19651e+06,2.24871e+06,2.15737e+06,2.35147e+06,2.26302e+06,2.19812e+06,2.0875e+06,2.03056e+06,1.96203e+06,2.06018e+06,2.11128e+06,2.10853e+06,2.13374e+06,2.10901e+06,2.26296e+06,2.0164e+06,2.09115e+06,2.03792e+06,2.12637e+06,2.14085e+06,2.16001e+06,2.2756e+06,2.24114e+06,2.41336e+06,2.2634e+06,2.34329e+06,2.2264e+06,2.33302e+06,2.24152e+06,1.9862e+06,2.15778e+06,2.2829e+06,2.23893e+06,2.11565e+06,2.30374e+06,2.30916e+06,2.5187e+06,2.43082e+06,2.55497e+06,2.46965e+06,2.27593e+06,2.17774e+06,2.12589e+06,2.26873e+06,2.03262e+06,2.2214e+06,2.12517e+06,2.04509e+06,2.08353e+06,2.09286e+06,2.16079e+06,2.33772e+06,2.02613e+06,1.79022e+06,2.08997e+06,1.96038e+06,2.10368e+06,1.86843e+06,2.35958e+06,2.30029e+06,2.51754e+06,2.20115e+06,1.67929e+06,2.36588e+06,2.47476e+06,2.4662e+06,2.48749e+06,2.4781e+06,2.6275e+06,2.55799e+06,2.65382e+06,2.43487e+06,2.49468e+06,2.816e+06,2.75566e+06,2.6963e+06,2.46098e+06,2.37014e+06,2.29009e+06,2.52903e+06,2.29543e+06,2.18464e+06,2.13366e+06,2.41024e+06,2.50854e+06,2.50553e+06,2.76764e+06,2.37775e+06,2.52245e+06,2.71848e+06,2.75275e+06,2.52737e+06,2.63753e+06,2.55228e+06,2.52562e+06,2.51038e+06,2.71144e+06,2.63684e+06,2.85916e+06,2.90703e+06,2.71774e+06,2.82557e+06,2.62867e+06,2.85164e+06,2.73356e+06,2.82692e+06,2.77557e+06,2.74124e+06,2.85076e+06,2.63225e+06,2.75354e+06,2.509e+06,2.71797e+06,2.63662e+06,2.69564e+06,2.91593e+06,2.93727e+06,3.01304e+06,2.8006e+06,2.67717e+06,2.74294e+06,3.05328e+06,2.9094e+06,2.80031e+06,2.62486e+06,2.57921e+06,2.71855e+06,2.67038e+06,2.8567e+06,2.82475e+06,3.10879e+06,2.86793e+06,2.91522e+06,2.75267e+06}"
    #data = data[1:-1]
    #print data
    data = [int(x) for x in data.split(',')]
    print data



def get_count_valid_pixel_year_dailies_rasdaman_geo_subset():
    # get query from txt file
    # make http request 46.44,56.44)
    #Longs = (-40.44,-31.44
    # parse response
    query = """for c in  (OC_CCI_V3_chlor_a_daily_final_test)
return 
encode (

coverage histogram over

$pansi ansi(imageCrsDomain(c[ansi("2002-01-01T00:00:00.000Z":"2002-12-31T00:00:00.000Z")], ansi))

values (

count(c[Lat(46.44:48.44),Long(-40.44:-38.44),ansi($pansi)] < 10000)
)
, "csv" )"""
    
    resp = requests.post('http://aurora.npm.ac.uk:8080/rasdaman/ows/wcps', data = {'query':query})
    data = resp.text
    #"{72711,209486,553488,414124,717472,1.38544e+06,546184,1.92304e+06,2.38247e+06,2.26598e+06,2.51428e+06,2.39622e+06,2.47347e+06,2.4621e+06,2.41684e+06,2.34636e+06,2.29236e+06,2.27524e+06,2.14153e+06,2.13028e+06,2.07111e+06,2.19592e+06,2.07689e+06,2.22208e+06,2.28352e+06,2.21018e+06,2.4767e+06,2.09365e+06,2.32056e+06,2.05412e+06,2.30236e+06,465816,2.15678e+06,2.25427e+06,1.41406e+06,1.24801e+06,1.80251e+06,2.15251e+06,2.18445e+06,2.14107e+06,2.11503e+06,1.91002e+06,2.16944e+06,2.12247e+06,2.17722e+06,2.21804e+06,2.34782e+06,2.1115e+06,2.12382e+06,2.11024e+06,2.32768e+06,1.94016e+06,2.19825e+06,2.45855e+06,2.33082e+06,2.55823e+06,2.26572e+06,2.46334e+06,2.18902e+06,2.31511e+06,2.15154e+06,2.21785e+06,2.13276e+06,2.30657e+06,2.14245e+06,2.19113e+06,2.06309e+06,1.60451e+06,2.20502e+06,2.18906e+06,2.24779e+06,2.25791e+06,2.3423e+06,2.28886e+06,2.28319e+06,2.43677e+06,2.13043e+06,2.24797e+06,2.18346e+06,2.17671e+06,2.14745e+06,2.29615e+06,2.152e+06,2.2513e+06,2.0865e+06,2.16888e+06,2.24366e+06,1.83247e+06,2.37524e+06,2.29223e+06,2.24434e+06,2.03229e+06,2.12637e+06,2.37571e+06,2.16999e+06,2.37027e+06,2.34807e+06,2.67477e+06,2.293e+06,2.12044e+06,2.14532e+06,2.03663e+06,2.06073e+06,2.062e+06,2.1806e+06,2.39518e+06,2.50413e+06,2.22647e+06,2.39676e+06,2.31182e+06,2.37906e+06,2.43791e+06,2.30384e+06,2.46389e+06,2.42558e+06,2.402e+06,2.45935e+06,2.28188e+06,1.92898e+06,2.14165e+06,2.19624e+06,2.18433e+06,2.33889e+06,2.15939e+06,2.38333e+06,2.26662e+06,2.25718e+06,2.23495e+06,2.45611e+06,2.30751e+06,2.41481e+06,2.46667e+06,2.43233e+06,2.63702e+06,2.21819e+06,2.44397e+06,2.51633e+06,2.65564e+06,2.3125e+06,2.65752e+06,2.53365e+06,2.61287e+06,2.59192e+06,2.70776e+06,2.39989e+06,2.3448e+06,2.49624e+06,2.42241e+06,2.5633e+06,2.46257e+06,2.45369e+06,2.39753e+06,2.53399e+06,2.4725e+06,2.52749e+06,2.67958e+06,2.56465e+06,2.45879e+06,2.51158e+06,2.41193e+06,2.52716e+06,2.68409e+06,2.62837e+06,2.80027e+06,2.53803e+06,2.4316e+06,2.64254e+06,2.70866e+06,2.74679e+06,2.72809e+06,2.61876e+06,2.2369e+06,2.34587e+06,2.22522e+06,2.37346e+06,2.39164e+06,2.39016e+06,2.54166e+06,2.49588e+06,2.46917e+06,2.4755e+06,2.42946e+06,2.33502e+06,2.33711e+06,2.34948e+06,2.21379e+06,2.34958e+06,2.34579e+06,2.40145e+06,2.42961e+06,2.6829e+06,2.5989e+06,2.51022e+06,2.35346e+06,2.58052e+06,2.5865e+06,2.54789e+06,2.54811e+06,2.53794e+06,2.73524e+06,2.37201e+06,2.34987e+06,2.35207e+06,2.49352e+06,2.31479e+06,2.31441e+06,2.3499e+06,2.37542e+06,2.47956e+06,2.29096e+06,2.18724e+06,2.2568e+06,2.31909e+06,2.25994e+06,2.23682e+06,2.24437e+06,2.35153e+06,2.31288e+06,2.54672e+06,2.51219e+06,2.36942e+06,2.35156e+06,2.36418e+06,2.36654e+06,2.38407e+06,2.34993e+06,2.14708e+06,2.11976e+06,2.32577e+06,2.28716e+06,2.33583e+06,2.19651e+06,2.24871e+06,2.15737e+06,2.35147e+06,2.26302e+06,2.19812e+06,2.0875e+06,2.03056e+06,1.96203e+06,2.06018e+06,2.11128e+06,2.10853e+06,2.13374e+06,2.10901e+06,2.26296e+06,2.0164e+06,2.09115e+06,2.03792e+06,2.12637e+06,2.14085e+06,2.16001e+06,2.2756e+06,2.24114e+06,2.41336e+06,2.2634e+06,2.34329e+06,2.2264e+06,2.33302e+06,2.24152e+06,1.9862e+06,2.15778e+06,2.2829e+06,2.23893e+06,2.11565e+06,2.30374e+06,2.30916e+06,2.5187e+06,2.43082e+06,2.55497e+06,2.46965e+06,2.27593e+06,2.17774e+06,2.12589e+06,2.26873e+06,2.03262e+06,2.2214e+06,2.12517e+06,2.04509e+06,2.08353e+06,2.09286e+06,2.16079e+06,2.33772e+06,2.02613e+06,1.79022e+06,2.08997e+06,1.96038e+06,2.10368e+06,1.86843e+06,2.35958e+06,2.30029e+06,2.51754e+06,2.20115e+06,1.67929e+06,2.36588e+06,2.47476e+06,2.4662e+06,2.48749e+06,2.4781e+06,2.6275e+06,2.55799e+06,2.65382e+06,2.43487e+06,2.49468e+06,2.816e+06,2.75566e+06,2.6963e+06,2.46098e+06,2.37014e+06,2.29009e+06,2.52903e+06,2.29543e+06,2.18464e+06,2.13366e+06,2.41024e+06,2.50854e+06,2.50553e+06,2.76764e+06,2.37775e+06,2.52245e+06,2.71848e+06,2.75275e+06,2.52737e+06,2.63753e+06,2.55228e+06,2.52562e+06,2.51038e+06,2.71144e+06,2.63684e+06,2.85916e+06,2.90703e+06,2.71774e+06,2.82557e+06,2.62867e+06,2.85164e+06,2.73356e+06,2.82692e+06,2.77557e+06,2.74124e+06,2.85076e+06,2.63225e+06,2.75354e+06,2.509e+06,2.71797e+06,2.63662e+06,2.69564e+06,2.91593e+06,2.93727e+06,3.01304e+06,2.8006e+06,2.67717e+06,2.74294e+06,3.05328e+06,2.9094e+06,2.80031e+06,2.62486e+06,2.57921e+06,2.71855e+06,2.67038e+06,2.8567e+06,2.82475e+06,3.10879e+06,2.86793e+06,2.91522e+06,2.75267e+06}"
    #data = data[1:-1]
    #print data
    data = [int(x) for x in data.split(',')]
    print data





def get_count_valid_pixel_geo_subset_year_dailies_xarray(geosubset):
   # find all files that are needed
   # loop each and open
   # extract area needed based on pixels
   # extract number of valid pixels
   # close file
   # create 'csv' to match ras format
   # end
   count = 0
   output = []
   #print "starting xarray subset test"
   for f in sorted(glob.glob(CCI_V3_DAILY_PATH+'/2002/*.nc')): #for future use the pixel cords for this year are 1553-1917
      #print "opening {file} for testing.....".format(file=f)
      ds  = xr.open_dataset(f)
      subset = ds.sel(lat=slice(*lat_bnds), lon=slice(*lon_bnds))
      count = count + 1
      output.append(subset['chlor_a'].count())
      if count>=365:
         break
   #print output


def copy_var(dsin,dsout,var,subset=None):
    for v_name, varin in dsin.variables.iteritems():
       if (v_name == var):
          outVar = dsout.createVariable(v_name, varin.datatype, varin.dimensions)
          print varin.dimensions
          print outVar.size
          print subset
          #print varin[subset[1]:subset[0]+1].size
    # Copy variable attributes
          outVar.setncatts({k: varin.getncattr(k) for k in varin.ncattrs()})
          if(subset):
             if(subset[0] > subset[1]): 
                outVar[:] = varin[subset[1]:subset[0]+1]
             else:
                outVar[:] = varin[subset[0]:subset[1]+1]
          else:
             outVar[:] = varin[:] 



def get_chl_where_rms_less_than_1_single_day():
   # find all files that are needed
   # loop each and open
   # extract both variables
   # close file
   # use numpy array where clause to create new chl array
   # write netcdf file
   # end
   count = 0
   output = []
   #print "starting normal numpy test"
   for f in sorted(glob.glob(CCI_V3_DAILY_PATH+'/2002/*.nc')): #for future use the pixel cords for this year are 1553-1917
      #print "opening {file} for testing.....".format(file=f)
      ncfile = nc.Dataset(f)
      outfile = nc.Dataset('testout.nc','w')
      copy_nc_dims_attr(ncfile,outfile)
      copy_var(ncfile,outfile,'lat')
      copy_var(ncfile,outfile,'lon')
      copy_var(ncfile,outfile,'time')
      chlor_a = ncfile.variables['chlor_a'][:]
      chlor_a_bias = ncfile.variables['chlor_a_log10_bias'][:]
      new_chlor_a = np.where(chlor_a_bias < 1,chlor_a,0)
      outVar = outfile.createVariable('new_chlor_a', ncfile.variables['chlor_a'].datatype, ncfile.variables['chlor_a'].dimensions)
      outVar.setncatts({k: ncfile.variables['chlor_a'].getncattr(k) for k in ncfile.variables['chlor_a'].ncattrs()})
      print chlor_a.shape
      print new_chlor_a.shape
      outfile.variables['new_chlor_a'][:] = new_chlor_a[:]
      #print chlor_a.count()
      ncfile.close()
      outfile.close()
      count = count + 1
      #output.append(chlor_a.count())
      if count>=1:
         break
   print output

def get_chl_where_rms_less_than_1_geo_subset_year_dailies():
       # find all files that are needed
   # loop each and open
   # extract both variables
   # close file
   # use numpy array where clause to create new chl array
   # write netcdf file
   # end
   count = 0
   output = []
   Lats = (40.44,48.44)
   Longs = (-8.44,0.44)
   #print "starting normal numpy test"
   for f in sorted(glob.glob(CCI_V3_DAILY_PATH+'/2002/*.nc')): #for future use the pixel cords for this year are 1553-1917
      #print "opening {file} for testing.....".format(file=f)
      ncfile = nc.Dataset(f)
      outfile = nc.Dataset('../output/netcdfs/testout{num}.nc'.format(num=str(count).zfill(3)),'w')
      lats = ncfile.variables['lat'][:]
      lons = ncfile.variables['lon'][:]
      subset = {}
      lat_idxs = subset['lat_idxs'] = [get_index(lats,Lats[0]),get_index(lats,Lats[1])]
      lon_idxs = subset['lon_idxs'] = [get_index(lons,Longs[0]),get_index(lons,Longs[1])]
      copy_nc_dims_attr(ncfile,outfile,lat_length=abs(subset['lat_idxs'][0] - subset['lat_idxs'][1] ),lon_length=abs(subset['lon_idxs'][0] - subset['lon_idxs'][1] ))


      copy_var(ncfile,outfile,'lat',subset=subset['lat_idxs'])
      copy_var(ncfile,outfile,'lon',subset=subset['lon_idxs'])
      copy_var(ncfile,outfile,'time')
      chlor_a = ncfile.variables['chlor_a'][0,lat_idxs[1]:lat_idxs[0]+1,lon_idxs[0]:lon_idxs[1]+1]
      chlor_a_bias = ncfile.variables['chlor_a_log10_bias'][0,lat_idxs[1]:lat_idxs[0]+1,lon_idxs[0]:lon_idxs[1]+1]
      new_chlor_a = np.where(chlor_a_bias < 1,chlor_a,0)
      outVar = outfile.createVariable('new_chlor_a', ncfile.variables['chlor_a'].datatype, ncfile.variables['chlor_a'].dimensions)
      outVar.setncatts({k: ncfile.variables['chlor_a'].getncattr(k) for k in ncfile.variables['chlor_a'].ncattrs()})
      print chlor_a.shape
      print new_chlor_a.shape
      outfile.variables['new_chlor_a'][:] = new_chlor_a[:]
      #print chlor_a.count()
      ncfile.close()
      outfile.close()
      count = count + 1
      #output.append(chlor_a.count())
      if count>=365:
         break
   print output 

def get_chl_where_rms_less_than_1_geo_subset_single_day():
   # find all files that are needed
   # loop each and open
   # extract both variables
   # close file
   # use numpy array where clause to create new chl array
   # write netcdf file
   # end
   count = 0
   output = []
   Lats = (40.44,48.44)
   Longs = (-8.44,0.44)
   #print "starting normal numpy test"
   for f in sorted(glob.glob(CCI_V3_DAILY_PATH+'/2002/*.nc')): #for future use the pixel cords for this year are 1553-1917
      #print "opening {file} for testing.....".format(file=f)
      ncfile = nc.Dataset(f)
      outfile = nc.Dataset('../output/netcdfs/testout{num}.nc'.format(num=str(count).zfill(3)),'w')
      lats = ncfile.variables['lat'][:]
      lons = ncfile.variables['lon'][:]
      subset = {}
      lat_idxs = subset['lat_idxs'] = [get_index(lats,Lats[0]),get_index(lats,Lats[1])]
      lon_idxs = subset['lon_idxs'] = [get_index(lons,Longs[0]),get_index(lons,Longs[1])]
      copy_nc_dims_attr(ncfile,outfile,lat_length=abs(subset['lat_idxs'][0] - subset['lat_idxs'][1] ),lon_length=abs(subset['lon_idxs'][0] - subset['lon_idxs'][1] ))


      copy_var(ncfile,outfile,'lat',subset=subset['lat_idxs'])
      copy_var(ncfile,outfile,'lon',subset=subset['lon_idxs'])
      copy_var(ncfile,outfile,'time')
      chlor_a = ncfile.variables['chlor_a'][0,lat_idxs[1]:lat_idxs[0]+1,lon_idxs[0]:lon_idxs[1]+1]
      chlor_a_bias = ncfile.variables['chlor_a_log10_bias'][0,lat_idxs[1]:lat_idxs[0]+1,lon_idxs[0]:lon_idxs[1]+1]
      new_chlor_a = np.where(chlor_a_bias < 1,chlor_a,0)
      outVar = outfile.createVariable('new_chlor_a', ncfile.variables['chlor_a'].datatype, ncfile.variables['chlor_a'].dimensions)
      outVar.setncatts({k: ncfile.variables['chlor_a'].getncattr(k) for k in ncfile.variables['chlor_a'].ncattrs()})
      print chlor_a.shape
      print new_chlor_a.shape
      outfile.variables['new_chlor_a'][:] = new_chlor_a[:]
      #print chlor_a.count()
      ncfile.close()
      outfile.close()
      count = count + 1
      #output.append(chlor_a.count())
      if count>=1:
         break
   print output 




def get_chl_where_rms_less_than_1_rasdaman_single_day():
    query = """for c in (OC_CCI_V3_chlor_a_merged_2002)
return
encode (
c[ansi("2002-05-01T00:00:00.000Z")].chlor_a *
(c[ansi("2002-05-01T00:00:00.000Z")].chlor_a_log10_bias < 1)
, "netcdf")"""
    resp = requests.post('http://aurora.npm.ac.uk:8080/rasdaman/ows/wcps', data = {'query':query})
    with open('get_chl_where_rms_less_than_1_rasdaman_single_day.nc', 'w') as outfile:
        outfile.write(resp.content)

def get_chl_where_rms_less_than_1_rasdaman_single_day_rev():
    query = """for c in (OC_CCI_V3_chlor_a_merged_2002)
return
encode (
c[ansi("2002-05-01T00:00:00.000Z")].chlor_a *
(c[ansi("2002-05-01T00:00:00.000Z")].chlor_a_log10_bias > 1)
, "netcdf")"""
    resp = requests.post('http://aurora.npm.ac.uk:8080/rasdaman/ows/wcps', data = {'query':query})
    with open('get_chl_where_rms_less_than_1_rasdaman_single_day_rev.nc', 'w') as outfile:
        outfile.write(resp.content)

def get_chl_where_rms_less_than_1_geo_subset_rasdaman_single_day():
    query = """for c in (OC_CCI_V3_chlor_a_merged_2002)
return
encode (
c[Lat(30:31),Long(-40:-39)][ansi("2002-05-01T00:00:00.000Z")].chlor_a *
(c[Lat(30:31),Long(-40:-39)][ansi("2002-05-01T00:00:00.000Z")].chlor_a_log10_bias < 1)
, "netcdf")"""
    resp = requests.post('http://aurora.npm.ac.uk:8080/rasdaman/ows/wcps', data = {'query':query})
    with open('get_chl_where_rms_less_than_1_geo_subset_rasdaman_single_day.nc', 'w') as outfile:
        outfile.write(resp.content)


def get_chl_where_rms_less_than_1_geo_subset_rasdaman_year_dailies():
    query = """for c in (OC_CCI_V3_chlor_a_merged_2002)
return
encode (
c[ansi("2002-01-01T00:00:00.000Z":"2002-12-31T00:00:00.000Z")].chlor_a *
(c[ansi("2002-01-01T00:00:00.000Z":"2002-12-31T00:00:00.000Z")].chlor_a_log10_bias < 1)
, "netcdf")"""
    resp = requests.post('http://aurora.npm.ac.uk:8080/rasdaman/ows/wcps', data = {'query':query})
    with open('get_chl_where_rms_less_than_1_geo_subset_rasdaman_year_dailies.nc', 'w') as outfile:
        outfile.write(resp.content)

def t():
    print "testingtimeit"



if __name__ == '__main__':
    #tt = timeit.timeit(get_count_valid_pixels_year_dailies_geo_subset, number=1)
    tt2 = timeit.timeit(get_chl_where_rms_less_than_1_geo_subset_year_dailies, number=1)

    #get_count_valid_pixels_year_dailies_xarray()
    #print "Python"
    #print tt
    print 'python'
    print tt2
    
    
    