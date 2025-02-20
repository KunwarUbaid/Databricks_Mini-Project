# Databricks notebook source
from pyspark import SparkConf , SparkContext
conf = SparkConf().setAppName("Mini Project")
sc = SparkContext.getOrCreate(conf = conf)

rdd = sc.textFile('/FileStore/tables/StudentData.csv')

rdd.collect()

# COMMAND ----------

#number of students in the file

#in order to do that first we have to remove the header present in the csv
headers = rdd.first()
rdd = rdd.filter(lambda x : x!=headers)

rdd.count()


# COMMAND ----------

#find total marks obtained by male and female
rdd2 = rdd.map(lambda x : (x.split(',')[1] , int(x.split(',')[5])))
rdd2.collect()


# COMMAND ----------

rdd3 = rdd2.reduceByKey(lambda x,y : x+y)
rdd3.collect()

# COMMAND ----------

#total students pass and failed 
#approach 1 -> filter out the rows whose marks greater than 50 and then give the count
passed = rdd.filter(lambda x : (int(x.split(',')[5])) > 50)
print("-----")
failed = rdd.filter(lambda x : (int(x.split(',')[5])) <= 50)
print(passed.count(),failed.count())

# COMMAND ----------

#total enrollment in course

rdd2 = rdd.map(lambda x : (x.split(',')[3],1))


enroll = rdd2.reduceByKey(lambda x,y : x+y)
enroll.collect()



# COMMAND ----------

#total marks achiever per course
rdd3 = rdd.map(lambda x : (x.split(',')[3] , int(x.split(',')[5])))
marks = rdd3.reduceByKey(lambda x,y : x+y)
marks.collect()



