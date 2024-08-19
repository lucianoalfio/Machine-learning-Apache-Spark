import sys

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import datetime


if __name__ == "__main__":

	if len(sys.argv) == 1:
		print('Please, inform filename <filename>', file=sys.stderr)
		sys.exit(-1)

	conf = SparkConf().setAppName("HdfsTest 3 Python")
# .set("spark.hadoop.yarn.resourcemanager.address", "192.168.1.15:8088")
	sc = SparkContext(conf=conf)

	lines = sc.textFile(sys.argv[1])
	linesfile = lines.collect()
	
	print ("Printing read file  | ", datetime.datetime.now())	
	print (" ==================================================" )
	for line in linesfile:
		print(line)
	
	print("Saving data in HDFS.. | ", datetime.datetime.now())
	print(" ======================================================" )
	lines.saveAsTextFile("/home/hadoop/mod5/output")	

	sc.stop()



