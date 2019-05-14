# This script determines if a parcel is within the Airport Travel Time Buffer
# Created by Puget Sound Regional Council Staff
# February 2019

import h5py
import pandas as pd 
import os
from shapely.geometry import Point
import geopandas as gp
import shutil

working_directory = os.getcwd()

seis_alternatives = ['base_yr','tfg_2050']

working_buffers = ["arlington_45",
                   "boeing_45",
                   "paine_45",
                   "seatac_45",
                   "tacoma_45"]

state_plane = 'epsg:2285'

def create_df_from_h5(h5_file, h5_table, h5_variables):

    h5_data = {}
    
    for var in h5_variables:
        h5_data[var] = h5_file[h5_table][var][:]
    
    return pd.DataFrame(h5_data)

def create_point_from_table(current_df,x_coord,y_coord,coord_sys):
    current_df['geometry'] = current_df.apply(lambda x: Point((float(x[x_coord]), float(x[y_coord]))), axis=1)
    geo_layer = gp.GeoDataFrame(current_df, geometry='geometry')
    geo_layer.crs = {'init' :coord_sys}
    
    return geo_layer

print ('Create a Parcel File with X,Y Coordinates to join final data with')
parcel_cols = ['parcelid','xcoord_p','ycoord_p']
parcel_file = working_directory+'/parcels/parcels_urbansim_'+seis_alternatives[1]+'.txt'
parcels_xy = pd.read_csv(parcel_file, sep = ' ')
parcels_xy.columns = parcels_xy.columns.str.lower()
parcels_xy = parcels_xy.loc[:,parcel_cols]

current_count = 0
for scenario in seis_alternatives:
    print ('Creating parcel file with people and jobs for the '+scenario+' scenario')
    
    person_variables=['hhno']
    hh_variables=['hhno','hhparcel']
    parcel_cols = ['parcelid','emptot_p']
    
    # Create Scenario Specific Filenames
    parcel_file = working_directory+'/parcels/parcels_urbansim_'+scenario+'.txt'
    hh_person = working_directory+'/parcels/hh_and_persons_'+scenario+'.h5'
    
    # Create a parcel dataframe
    wrk_prcls = pd.read_csv(parcel_file, sep = ' ')
    wrk_prcls.columns = wrk_prcls.columns.str.lower()
    wrk_prcls = wrk_prcls.loc[:,parcel_cols]

    # Create HH and Person dataframes from the h5 File
    hh_people = h5py.File(hh_person,'r+') 
    hh_df = create_df_from_h5(hh_people, 'Household', hh_variables)
    person_df = create_df_from_h5(hh_people, 'Person', person_variables)

    # Create a HH file by household number with total population
    person_df['population'] = 1
    df_hh = person_df.groupby('hhno').sum()
    df_hh = df_hh.reset_index()

    # Merge the HH File created from the persons with the original HH file
    df_hh = pd.merge(df_hh,hh_df,on='hhno',suffixes=('_x','_y'),how='left')
    df_hh.rename(columns={'hhparcel': 'parcelid'}, inplace=True)
    fields_to_remove=['hhno']
    df_hh = df_hh.drop(fields_to_remove,axis=1)

    # Group the HH Files by Parcel ID so it can be merged with master parcel file
    df_parcel_hh = df_hh.groupby('parcelid').sum()
    df_parcel_hh = df_parcel_hh.reset_index()

    # Merge the Full Parcel File with X,Y with the parcel file from the HH's
    wrk_prcls = pd.merge(wrk_prcls,df_parcel_hh,on='parcelid',suffixes=('_x','_y'),how='left')
    wrk_prcls.fillna(0,inplace=True)
    wrk_prcls.rename(columns={'emptot_p': 'employment'}, inplace=True)
    updated_columns = ['parcelid',scenario + '_jobs', scenario + '_people']
    wrk_prcls.columns = updated_columns

    if current_count == 0:
        initial_parcels = wrk_prcls
            
    else:
        initial_parcels = pd.merge(initial_parcels, wrk_prcls, on='parcelid',suffixes=('_x','_y'),how='left')
    
    current_count = current_count + 1

# Merge the parcels with X,Y datafame and create a column for airport buffer flag
initial_parcels = pd.merge(initial_parcels, parcels_xy, on='parcelid',suffixes=('_x','_y'),how='left')
initial_parcels.fillna(0,inplace=True)
initial_parcels['airport_buffer'] = 0

print ('Creating a parcel layer from the x,y to spatial join with the drive times')
parcels_layer = create_point_from_table(initial_parcels,'xcoord_p','ycoord_p',state_plane)

current_count = 0
for buffer_name in working_buffers:
     
    parcels = initial_parcels

    drive_time_shp = working_directory+'/jb/'+buffer_name+'.shp'
    drive_time_prj = working_directory+'/jb/'+buffer_name+'.prj'

    print ('Creating a full Airport ID list to iterate over for drive times')
    airports_df = gp.GeoDataFrame.from_file(drive_time_shp)
    columns_to_keep=['LOCID']
    airports_df = airports_df.loc[:,columns_to_keep]

    for rows in range(0, (len(airports_df))):
        print ('Working on Airport ' + str(rows+1) + ' of ' + str(len(airports_df)))
        curr_airport_num = airports_df['LOCID'][rows]
        interim = os.path.join(working_directory,'current_airport.shp')
        df = gp.GeoDataFrame.from_file(drive_time_shp)
        df[df['LOCID']==airports_df['LOCID'][rows]].to_file(interim)
    
        # Shapefile of the Current Airport to Add an ID to the Parcels Table
        working_airport = gp.GeoDataFrame.from_file(interim)
        working_airport_projection = os.path.join(working_directory, 'current_airport.prj')
        shutil.copyfile(drive_time_prj, working_airport_projection)

        # Open join shapefile as a geodataframe
        join_layer = gp.GeoDataFrame.from_file(interim)
        join_layer.crs = {'init' :state_plane}
    
        print ('Spatial joining the parcels layer with Airport # ' + str(curr_airport_num) )
        keep_columns = ['parcelid','working_buffer']
        merged = gp.sjoin(parcels_layer, join_layer, how = "inner", op='intersects')
        merged = pd.DataFrame(merged)
        merged['working_buffer'] = 1
        merged = merged[keep_columns] 
    
        print ('Merged the Airport #' + str(curr_airport_num) + ' with the full parcels table')
        parcels = pd.merge(parcels, merged, on='parcelid',suffixes=('_x','_y'),how='left')
        parcels.fillna(0,inplace=True)
        parcels.loc[parcels['working_buffer'] == 1, 'airport_buffer'] = 1
        parcels  = parcels.drop(columns=['working_buffer'])
    
    cols = ['xcoord_p','ycoord_p','geometry']
    parcels  = parcels.drop(cols,axis=1)

    print ('Summarizing Population and Employment by Airport Buffer')
    working_df = parcels
    working_df = working_df.groupby('airport_buffer').sum()
    working_df = working_df.reset_index()
    working_df = working_df.loc[working_df['airport_buffer'] > 0]
    working_df  = working_df.drop(columns=['parcelid'])
    
    if current_count == 0:
        final_df = working_df
            
    else:
        final_df = final_df.append(working_df)
    
    current_count = current_count + 1
  
final_df.to_csv(working_directory + '/airport_buffer_45mins.csv',index=False)
