import arcpy
import os
import time
import pandas as pd

os.chdir(r"G:\UCalgary Research\Network_analysis")
arcpy.env.overwriteOutput = True
arcpy.CheckOutExtension("network")
arcpy.env.parallelProcessingFactor = "8"
arcpy.ImportToolbox("large-network-analysis-tools-master/LargeNetworkAnalysisTools.pyt")

### Part 1 -- Shortest path between each OD pair ###

# parameters
maxChunk = 1000
ncores = 8
outOrig = "Network_within_city.gdb/within_city_SolveLargeODCostMatrix"
outDest = "Network_within_city.gdb/within_city_SolveLargeODCostMatrix1"
outFolder = "LOD_within_city/"

# trip_path = "red_deer_trips.csv"

city_path = "Network_within_city.gdb/City_zone_AB_4326"
city_layer = "city_4326"
city_name_field = 'GEONAME'

zone_cent_path = "Network_within_city.gdb/Zones_AB_sj"
zone_cent_layer = "city_cent"
zone_name_field = "REGION"

# csv_out_path = "red_deer_trips_pre.csv"

# list of city names
city_name_ls = []

with arcpy.da.SearchCursor(city_path, [city_name_field]) as cursor:
    for row in cursor:
        city_name_ls.append(row[0])
        
def main():
    
    for ct in city_name_ls:
    
        start_time = time.time()
        outCityFolder = outFolder + ct.replace(" ", "_")
        
        # Make a layer from the feature class - zone cent
        arcpy.management.MakeFeatureLayer(zone_cent_path, zone_cent_layer, "{} = {}".format(zone_name_field,ct))
        
        # Solve Large OD Cost Matrix by parallel
        arcpy.LargeNetworkAnalysisTools.SolveLargeODCostMatrix(zone_cent_layer, zone_cent_layer, "NetworkDataSet", "Driving distance", 
                                                                "Minutes", "Kilometers", maxChunk, ncores, outOrig, outDest, 
                                                                "CSV files", None, outCityFolder, None, None, None, True)
        
        # # join dataframes
        # trip_df = pd.read_csv(trip_path, sep=',')
        # trip_df = trip_df.set_index(['Origin','Destination'])
        # route_path = outCityFolder + '\\' + os.listdir(outCityFolder)[0]
        # route_df = pd.read_csv(route_path, sep=',')
        # route_df = route_df.set_index(['Origin','Destination'])
        # merge_df = route_df.join(trip_df)
        
        # # output results
        # out_path = csv_out_path
        # merge_df.to_csv(out_path, index=True)
        
        print ("City: {}".format(ct))
        print ("Part 1 processing time: %s seconds" % (time.time() - start_time))

if __name__=='__main__':
    main()
