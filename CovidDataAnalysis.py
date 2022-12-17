import sys
from random import random
from operator import add
from pyspark.sql import SQLContext, Row
from pyspark import SparkContext
from pyspark.sql import DataFrameWriter

if __name__ == "__main__":
	sc = SparkContext(appName="CovidDataAnalysis")
	sqlContext = SQLContext(sc)
	file = []
	file1 = "hdfs://covid_19_clean_complete.csv"
	file2 = "hdfs://covid_19_data.csv"
	file3 = "hdfs://migration_population.csv"
	file.append(file1)
	file.append(file2)
	file.append(file3)
	for file in file:
		Parselines = sc.textFile(file)
		SplitLine = Parselines.map(lambda l: l.split(","))
		val = SplitLine.map(lambda p: Row(ID=p[0], DATE=p[1], ELEMENT=p[2], VALUE=int(p[3]), MF=p[4], QF=p[5], SF=p[6]))
		coviddata = sqlContext.createDataFrame(val)
		coviddata.registerTempTable("covid")
		analysisone = sqlContext.sql("select *, to_date(Date, 'yyyy-MM-dd') as fdate from `countries-aggregated`")
		analysisone.show()
		analysistwo = sqlContext.sql("SELECT Country, IF(Country='US',MAX(Confirmed), MIN(Confirmed)) Confirmed from `countries-aggregated` where Country in ('US','China') AND VALUE <> 9999 group by Country")
		analysistwo.show()
		analysisthreeA = sqlContext.sql("SELECT Country, MIN(Confirmed),MIN(Recovered),MIN(Death) from `countries-aggregated` GROUP BY Country ORDER BY Country DESC LIMIT 5")
		analysisthreeA.show()
		analysisthreeB = sqlContext.sql("SELECT Country, MAX(Confirmed),MAX(Recovered),MAX(Death) from `countries-aggregated` GROUP BY Country ORDER BY Country DESC LIMIT 5")
		analysisthreeB.show()
		analysisfourA = sqlContext.sql("SELECT fdate, Country from `countries-aggregated` where Country IN ('US','Spain','Italy','France','Germany') group by Fdate ORDER BY fdate LIMIT 100")
		analysisfourA.show()		
		analysisone.rdd.map(lambda x: ",".join(map(str,x))).coalesce(1).saveAsTextFile("hdfs:/user/shriya/testing1/"+str(num)+"result.csv")
		analysistwo.rdd.map(lambda x: ",".join(map(str,x))).coalesce(1).saveAsTextFile("hdfs:/user/shriya/testing2/"+str(num)+"result.csv")
		analysisthreeA.rdd.map(lambda x: ",".join(map(str,x))).coalesce(1).saveAsTextFile("hdfs:/user/shriya/testing3_1/"+str(num)+"result.csv")
		analysisthreeB.rdd.map(lambda x: ",".join(map(str,x))).coalesce(1).saveAsTextFile("hdfs:/user/shriya/testing3_2/"+str(num)+"result.csv")
		analysisfourA.map(lambda x: ",".join(map(str,x))).coalesce(1).saveAsTextFile("hdfs:/user/shriya/testing4_1/"+str(num)+"result.csv")
		
	sc.stop()


