import xarray as xr
import matplotlib.pyplot as plt
from matplotlib.colors import LogNorm
import numpy as np

CCI_V3_DAILY_PATH='/data/datasets/CCI/v3.0-release/geographic/netcdf/daily/chlor_a/' # /years/365_files
CCI_V3_MONTHLY_PATH='/data/datasets/CCI/v3.0-release/geographic/netcdf/monthly/chlor_a' # /years/12_files



ncfile = CCI_V3_DAILY_PATH+'2002/ESACCI-OC-L3S-CHLOR_A-MERGED-1D_DAILY_4km_GEO_PML_OCx-20021207-fv3.0.nc'
ncfile_month = CCI_V3_MONTHLY_PATH+'/2002/ESACCI-OC-L3S-CHLOR_A-MERGED-1M_MONTHLY_4km_GEO_PML_OCx-200206-fv3.0.nc'

ds = xr.open_dataset(ncfile_month)

#print ds

lat_bnds = [70, 0]
lon_bnds =  [-20, 10]
subset = ds.sel(lat=slice(*lat_bnds), lon=slice(*lon_bnds))
# out = subset['chlor_a'].plot(norm=LogNorm())
# plt.show()
print ds['chlor_a'].count()
print subset['chlor_a'].count()