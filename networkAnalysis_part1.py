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
outOrig = "Network_red_deer.gdb/red_deer_cent_SolveLargeODCostMatrix"
outDest = "Network_red_deer.gdb/red_deer_cent_SolveLargeODCostMatrix1"
outFolder = "LOD_red_deer"
trip_path = "red_deer_trips.csv"
zone_cent_path = "Network_red_deer.gdb/red_deer_cent"
zone_cent_layer = "red_deer_cent"
csv_out_path = "red_deer_trips_pre.csv"

def main():
    
    start_time = time.time()
    
    # Make a layer from the feature class - zone cent
    arcpy.MakeFeatureLayer_management(zone_cent_path,zone_cent_layer)
    
    # Solve Large OD Cost Matrix by parallel
    arcpy.LargeNetworkAnalysisTools.SolveLargeODCostMatrix(zone_cent_layer, zone_cent_layer, "NetworkDataSet", "Driving distance", 
                                                            "Minutes", "Kilometers", maxChunk, ncores, outOrig, outFolder, 
                                                            "CSV files", None, outFolder, None, None, None, True)
    
    # join dataframes
    trip_df = pd.read_csv(trip_path, sep=',')
    trip_df = trip_df.set_index(['Origin','Destination'])
    route_path = outFolder + '\\' + os.listdir(outFolder)[0]
    route_df = pd.read_csv(route_path, sep=',')
    route_df = route_df.set_index(['Origin','Destination'])
    merge_df = route_df.join(trip_df)
    
    # output results
    out_path = csv_out_path
    merge_df.to_csv(out_path, index=True)
    
    print ("Part 1 processing time: %s seconds" % (time.time() - start_time))

if __name__=='__main__':
    main()
