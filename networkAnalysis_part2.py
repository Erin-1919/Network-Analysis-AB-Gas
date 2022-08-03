import arcpy
import os
import time
import functools
import numpy as np
import pandas as pd
import multiprocess as mp

os.chdir(r"G:\UCalgary Research\Network_analysis")
arcpy.env.overwriteOutput = True
arcpy.CheckOutExtension("network")
arcpy.env.parallelProcessingFactor = "8"

### Part 2 -- Calculate travel distance by visiting nearby gas stations for each OD pair ###

# parameters
ncores = 8
buffer_dist = 10
buffer_dist_km = "{} Kilometers".format(buffer_dist)

gas_station_path = "Network_analysis.gdb/Gas_station_AB_4326"
gas_station_layer = "Gas_station_AB_4326"
gas_ID_field = "GasStatID"

zone_cent_path = "Network_analysis.gdb/Zones_AB_cent_4326"
zone_cent_layer = "zone_AB_cent_4326"
zone_ID_field = "TAZ_ID"

city_path = "Network_within_city.gdb/City_zone_AB_4326"
city_layer = "city_4326"
city_name_field = 'GEONAME'

nds = "DMTI_AB_network/canmapcontentsuite.gdb/Transportation/NetworkDataSet"
nd_layer_name = "NetworkDataSet"
dist_field = "Total_Kilometers"

# csv_in_path = "red_deer_trips_pre.csv"
# csv_out_path = "red_deer_trips_out_test2.csv"
# csv_clean_out_path = "red_deer_trips_out2_test2.csv"
outFolder = "LOD_within_city/"

fc = "stops"
in_memory_fc = "memory/stops"
spatial_ref = arcpy.Describe(gas_station_path).spatialReference


def gas_dist_df(df,gas_ls):
    
    # Make a feature class in memory to store stops
    arcpy.CreateFeatureclass_management("memory", fc, "POINT", "", "DISABLED", "DISABLED", spatial_ref)
    arcpy.AddField_management(in_memory_fc, "class", "TEXT", 20)
          
    # Create a network dataset layer and get the desired travel mode for analysis
    arcpy.nax.MakeNetworkDatasetLayer(nds, nd_layer_name)
    nd_travel_modes = arcpy.nax.GetTravelModes(nd_layer_name)
    travel_mode = nd_travel_modes["Driving distance"]
    
    # Instantiate a Route solver object
    route = arcpy.nax.Route(nd_layer_name)
    
    # Set properties
    route.findBestSequence = False
    route.timeUnits = arcpy.nax.TimeUnits.Minutes
    route.travelMode = travel_mode
    route.routeShapeType = arcpy.nax.RouteShapeType.NoLine
    
    # prepare OD pair and gas station lists
    # skip same zone trips
    OD_pair = [(o,d) for o,d in list(df.index.values)]
    
    for p in OD_pair:
        o,d = p[0],p[1]
        distDict = {}
        
        # skip same zone trips
        if o != d:
            
            # geometry info of origin and destination
            where_clause = "{} = {} Or {} = {}".format(zone_ID_field,o,zone_ID_field,d)
            
            rowValues = []
            
            with arcpy.da.SearchCursor(zone_cent_layer, 'SHAPE@XY', where_clause) as cursorSearch:
                for row in cursorSearch:
                    rowValues.append(('od', row[0]))
            
            n = 1
            
            # loop through all selected gas stations
            for g in gas_ls:
                
                # geometry info of gas station
                with arcpy.da.SearchCursor(gas_station_layer, 'SHAPE@XY', "{} = {}".format(gas_ID_field,g)) as cursorSearch:
                    for row in cursorSearch:
                        gas_geom = row[0]
                        gas = ('gas', gas_geom)
                
                # for the first gas, add geometry info of gas into row value list at index 1
                if n == 1:
                    
                    rowValues.insert(1, gas)
                    
                    with arcpy.da.InsertCursor(in_memory_fc, ('class','SHAPE@XY')) as cursorInsert:
                        for row in rowValues:
                            cursorInsert.insertRow(row)
                
                # for the left gas, update geometry info with each gas station
                else:
                    
                    with arcpy.da.UpdateCursor(in_memory_fc, ['class','SHAPE@XY'], "class = {}".format("'gas'")) as cursorUpdata:
                        for row in cursorUpdata:
                            row[1] = gas_geom
                            cursorUpdata.updateRow(row) 
                    
                n += 1
                
                # Load inputs                
                route.load(arcpy.nax.RouteInputDataType.Stops, in_memory_fc, append = False)
                               
                # Solve the analysis
                result = route.solve()
                
                # Export the results
                if result.solveSucceeded:
                    for row in result.searchCursor(arcpy.nax.RouteOutputDataType.Routes, dist_field):
                        # print("Solved -- Origin: {} -> Gas: {} -> Destination: {}".format(o,g,d))
                        distDict[g] = row[0]
                else:
                    # print("Failed -- Origin: {} -> Gas: {} -> Destination: {}".format(o,g,d))
                    # print(result.solverMessages(arcpy.nax.MessageSeverity.All))
                    pass
   
        # update the dataframe cell
        distDict = dict(sorted(distDict.items(), key=lambda item: item[1]))
        df.loc[p,'Gas_dict'] = [distDict]
        
        # delete all rows in the in memory feature class
        arcpy.management.DeleteRows(in_memory_fc)
        
    return df        


def main(city):
    
    start_time = time.time()
    
    # read csv
    outCityFolder = outFolder + city.replace(" ", "_")
    csv_in_path = outCityFolder + '\\' + os.listdir(outCityFolder)[0]
    csv_out_path = outCityFolder + city.replace(" ", "_") + ".csv"
    csv_clean_out_path = outCityFolder + city.replace(" ", "_") + "_clean.csv"
    
    merge_df = pd.read_csv(csv_in_path, sep=',')
    merge_df = merge_df.set_index(['Origin','Destination'])
    merge_df['Gas_dict'] = np.nan
    
    # Make layers from the feature classes - gas station + zone cent + city
    arcpy.MakeFeatureLayer_management(gas_station_path,gas_station_layer)
    arcpy.MakeFeatureLayer_management(zone_cent_path,zone_cent_layer)
    arcpy.MakeFeatureLayer_management(city_path,city_layer)
    
    # find out gas stations within buffer distance of a certain city boundary
    where_clause = " 'GEONAME' = {}".format(city)
    arcpy.management.SelectLayerByAttribute(city_layer, "NEW_SELECTION", where_clause, None)
                
    # select by location within buffer distance
    arcpy.management.SelectLayerByLocation(gas_station_layer, "INTERSECT", city_layer, buffer_dist_km, "NEW_SELECTION", "NOT_INVERT")
    
    # get a list of gas station ID
    gas_ls = arcpy.da.TableToNumPyArray(gas_station_layer, gas_ID_field).astype(int).tolist()

    # # run sequentially
    # merge_df = gas_dist_df(merge_df)
    
    # run in parallel
    gas_dist_df_p = functools.partial(gas_dist_df,gas_ls = gas_ls)
    
    merge_df_split = np.array_split(merge_df, ncores)
    pool = mp.Pool(processes = ncores)
    merge_df = pd.concat(pool.map(gas_dist_df_p, merge_df_split))
    pool.close()
    pool.join()
    
    # found gas station counts
    merge_df['Gas_count'] = [len(dic) for dic in merge_df['Gas_dict']]
    merge_df = merge_df[['Total_Distance','Trips','Gas_count','Gas_dict']]
    
    print ("City: {}".format(city))
    print ("Part 2 processing time: %s seconds" % (time.time() - start_time))
    
    # output results
    merge_df.to_csv(csv_out_path, index=True)
    
    # a bit clean-up of outputs 
    with open(csv_out_path,'r') as file:
        data = file.read().replace(":",",").replace("{}", "").replace('"{', "").replace('}"', "")

    with open(csv_clean_out_path,'w') as file:
        file.write(data)


if __name__=='__main__':
    
    # list of city names
    city_name_ls = []
    
    with arcpy.da.SearchCursor(city_path, [city_name_field]) as cursor:
        for row in cursor:
            city_name_ls.append(row[0])
    
    for ct in city_name_ls:
        main(ct)



# with arcpy.da.SearchCursor("memory/stops", 'SHAPE@XY') as cur:
#     for row in cur:
#         print (row[0])

