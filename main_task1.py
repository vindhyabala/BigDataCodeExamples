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
        if (isfloat(p[5]) and isfloat(p[11])):
            # Remove records where trip distance = 0 and/or fare amount = 0
            if (float(p[5]) != 0 and float(p[11]) != 0):
                return p


if __name__ == "__main__":
    if len(sys.argv) != 3:
       print("Usage: Taxi <in_file> <output_file> ", file=sys.stderr)
       exit(-1)

    sc = SparkContext(appName="TaxiDataAnalysis")
    lines = sc.textFile(sys.argv[1]) #"taxi-data-sorted-small.csv.bz2")
    taxi_lines = lines.map(lambda x: x.split(','))
    taxi_lines_cleaned = taxi_lines.filter(fixRows)

    # Get count of distinct pairs of taxi medallion and license
    taxidriverpairs = taxi_lines_cleaned.map(lambda p: (p[0], p[1])).distinct().countByKey()

    # Select top 10
    top10 = sorted(taxidriverpairs.items(), key=operator.itemgetter(1), reverse=True)[:10]

    # Compile and write output to a single file
    datatofile = sc.parallelize(top10).coalesce(1)
    datatofile.saveAsTextFile(sys.argv[2]) #"top10vbfinal")

    sc.stop()
