from pyspark.sql import SparkSession

spark = SparkSession.builder.enableHiveSupport().getOrCreate()

spark.sparkContext.setLogLevel('ERROR')

spark.sql("use indiaDB")

data = spark.sql("select * from suicides")

print("\n--------------0.Verifying the data from Hive Table--------------")
data.show(20,False)

data.createOrReplaceTempView("suicides")

from pyspark.sql.functions import sum,col,min,max


print("--------------1.List of States in the Dataset--------------")

spark.sql("select distinct(state) as STATES from suicides order by STATES").show(40,False)


print("--------------2.Total Cases according to the Year--------------")

query1 = spark.sql("select type_code, year, sum(case_count) as Total from suicides where group by type_code,year order by year").filter(col("type_code")=="Means_adopted").drop("type_code")
query1.show(40,False)


print("--------------3.Percentage of increase in suicides between 2001 and 2012--------------\n")

maxVal = query1.select(max("Total")).collect()[0][0]
minVal = query1.select(min("Total")).collect()[0][0] 
print("Average increase: " + str((float(maxVal)-float(minVal))/float(minVal)*100))


print("\n--------------4.Cases every year gender wise with %share of both gender--------------")

maleData = data.filter((data.type_code == "Means_adopted") & (data.gender == "Male")).groupBy("year","gender").agg(sum("case_count").alias("Male")).orderBy("year").drop("gender")
femaleData = data.filter((data.type_code == "Means_adopted") & (data.gender == "Female")).groupBy("year","gender").agg(sum("case_count").alias("Female")).orderBy("year").drop("gender")
genderData = maleData.join(femaleData,"year")
genderData.withColumn("Male %", col("Male")/(col("Male")+col("Female"))*100).withColumn("Female %" ,col("Female")/(col("Male")+col("Female"))*100).show()


print("--------------5.Causes for suicide in descending order of total of cases--------------")

data.filter(data.type_code == "Causes").groupBy("type").agg(sum("case_count").alias("Total_Cases")).orderBy(col("Total_Cases").desc()).show(50,False)


print("--------------6.Causes for suicide based on age groups in descending order of total cases--------------")

data.filter(data.type_code == "Causes").groupBy("type","age_group").agg(sum("case_count").alias("Total_Cases")).orderBy(col("Total_Cases").desc()).show(200,False)


print("--------------7.Total cases categorised under cause of suicide gender wise--------------")

maleData = data.filter((data.type_code == "Causes") & (data.gender == "Male")).groupBy("type").agg(sum("case_count").alias("Male")).orderBy(col("Male").desc())
femaleData = data.filter((data.type_code == "Causes") & (data.gender == "Female")).groupBy("type").agg(sum("case_count").alias("Female")).orderBy(col("Female").desc())
genderData = maleData.join(femaleData,"type")
genderData.show(50,False)


print("--------------8.Total cases categorised under cause of suicide of specific age group--------------")

data.filter((data.type_code == "Causes") & (data.age_group == "15-29")).groupBy("type").agg(sum("case_count").alias("Age 15-29")).orderBy(col("Age 15-29").desc()).show(20,False)


print("--------------9.Total cases categorised under professional profile of the people who committed suicide--------------")

data.filter(data.type_code == "Professional_Profile").groupBy("type").agg(sum("case_count").alias("Total_Cases")).orderBy(col("Total_Cases").desc()).show(50,False)


print("--------------10.Total cases categorised under method adopted for suicide--------------")

data.filter(data.type_code == "Means_adopted").groupBy("type").agg(sum("case_count").alias("Total_Cases")).orderBy(col("Total_Cases").desc()).show(50,False)


print("--------------11.Total cases categorised under state--------------")

data.filter(data.type_code == "Means_adopted").groupBy("state").agg(sum("case_count").alias("Total_Cases")).orderBy(col("Total_Cases").desc()).show(50,False)


print("--------------12.Cases on the causes of suicide for Maharashtra--------------")

data.filter((data.type_code == "Causes") & (data.state == "Maharashtra")).groupBy("state","type").agg(sum("case_count").alias("Total_Cases")).orderBy(col("Total_Cases").desc()).drop("state").show(50,False)


print("--------------13.Cases on the education status of the people committed suicide--------------")

data.filter(data.type_code == "Education_Status").groupBy("type").agg(sum("case_count").alias("Total_Cases")).orderBy(col("Total_Cases").desc()).show(50,False)

print("\n--------------14.Creating Partitions based on state and type_code--------------\n")
spark.sparkContext.setLogLevel('INFO')
data.write.option("header",True).partitionBy("state","type_code").mode("overwrite").csv("/suicideData")
spark.sparkContext.setLogLevel('ERROR')

print("\n--------------15.Reading data from partition and comparing two states based on cause for suicide--------------")

MHData = spark.read.options(header='True', inferSchema='True').csv("/suicideData/state=Maharashtra/type_code=Causes/").withColumnRenamed("case_count", "MH Cases")
WBData = spark.read.options(header='True', inferSchema='True').csv("/suicideData/state=West Bengal/type_code=Causes/").withColumnRenamed("case_count", "WB Cases")
outData = MHData.join(WBData,['year','type','gender','age_group'])
outData.groupBy('type').agg(sum("MH Cases").alias("Cases in Maharashtra"), sum("WB Cases").alias("Cases in West Bengal")).orderBy(col("Cases in Maharashtra").desc(),col("Cases in West Bengal").desc()).withColumnRenamed("type","Causes for Suicide").show(50,False)


print("--------------16.Reading data from partition and comparing two states based on method adopted for  suicide--------------")

WBData = spark.read.options(header='True', inferSchema='True').csv("/suicideData/state=West Bengal/type_code=Means_adopted/").withColumnRenamed("case_count", "WB Cases")
MHData = spark.read.options(header='True', inferSchema='True').csv("/suicideData/state=Maharashtra/type_code=Means_adopted/").withColumnRenamed("case_count", "MH Cases")
outData = MHData.join(WBData,['year','type','gender','age_group'])
outData.groupBy('type').agg(sum("MH Cases").alias("Cases in Maharashtra"), sum("WB Cases").alias("Cases in West Bengal")).orderBy(col("Cases in Maharashtra").desc(),col("Cases in West Bengal").desc()).withColumnRenamed("type","Means Adopted for Suicide").show(50,False)


print("--------------17.Trend of a particular cause between the timeframe along with percentage contribution--------------")

totalValue = data.filter(data.type == "Drug Abuse/Addiction").groupBy("year").agg(sum("case_count").alias("TC")).agg(sum("TC")).collect()[0][0]
data.filter(data.type == "Drug Abuse/Addiction").groupBy("year").agg(sum("case_count").alias("TC")).orderBy("year").withColumn("% Share of Total (" +str(totalValue)+ ")",(col("TC")/totalValue)*100).withColumnRenamed("TC", "Drug Abuse/Addiction Cases").show(50,False)


print("--------------18.Division of suicide cases in various age groups for Students--------------")

data.filter((data.type == "Student")).groupBy("type","age_group").agg(sum("case_count").alias("Student Cases")).orderBy(col("age_group").desc()).drop("type").show(20,False)


print("--------------19.Division of suicide cases in various age groups for Failure in Examination--------------")

data.filter((data.type == "Failure in Examination")).groupBy("type","age_group").agg(sum("case_count").alias("Student Cases")).orderBy(col("age_group").desc()).drop("type").show(20,False)
