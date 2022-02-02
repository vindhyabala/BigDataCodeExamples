from __future__ import print_function

import sys, operator
from operator import add

from pyspark import SparkContext

# Function to check if argument is of float datatype
def isfloat(val):
    try:
        float(val)
        return True
    except:
        return False

# Function to clean up data
def fixRows(p):
    # Remove any customer records that don't have all 17 attributes.
    if (len(p) == 17):
        if (isfloat(p[4]) and isfloat(p[16])):
            # Remove records where trip distance = 0 and/or fare amount = 0
            if (float(p[4]) != 0 and float(p[16]) != 0):
                return p


if __name__ == "__main__":
    if len(sys.argv) != 3:
       print("Usage: Taxi <in_file> <output_file> ", file=sys.stderr)
       exit(-1)

    sc = SparkContext(appName="TaxiDriverAnalysis")
    lines = sc.textFile(sys.argv[1])#"taxi-data-sorted-small.csv.bz2")
    taxi_lines = lines.map(lambda x: x.split(','))
    taxi_lines_cleaned = taxi_lines.filter(fixRows)

    # Calculate profit ratio for each driver
    ## Select total amount and trip duration for each driver
    driver_metrics = taxi_lines_cleaned.map(lambda p: (p[1],(int(p[4]), 60 * float(p[16]))))
    ## Profit ratio = Total Trip Duration in min / Total Fare Amount
    driver_profit = driver_metrics.reduceByKey(lambda x,y: (x[0]+y[0],x[1]+y[1])).mapValues(lambda a: round(a[1]/a[0]))

    # Select top 10 drivers with best profit ratios
    top10drivers = driver_profit.top(10,key = lambda p:p[1])

    # Compile and write output to a single file
    datatofile = sc.parallelize(top10drivers).coalesce(1)
    datatofile.saveAsTextFile(sys.argv[2])#"top10driversvb5")

    sc.stop()
