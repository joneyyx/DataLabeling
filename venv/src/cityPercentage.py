#-*- coding: UTF-8 -*-
from pyspark.sql.functions import *
from pyspark.sql import SparkSession, Window
from pyspark.sql.types import *
from pyspark import *

spark = SparkSession.builder.appName("data_Label").enableHiveSupport().getOrCreate()
spark.conf.set("spark.sql.shuffle.partitions", 400)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
spark.conf.set("spark.sql.broadcastTimeout", 36000)
spark.conf.set("spark.rpc.askTimeout", 300)
spark.conf.set("spark.dynamicAllocation.enabled", "true")

def main():
    data = spark.sql("select engine_serial_number, occurrence_date_time ,province,high_resolution_total_vehicle_distance,wheel_based_vehicle_speed ,gps_vehicle_speed, highway_code, roadname, report_date from data_label.tel_hb_labeled where report_date = '20200701'")

    # if we calculate based on formatted roadname, we can not ignore the null count number
    formatRoadDF = data.withColumn("formatted_road", when(col("roadname").isNull(), lit("unknown")).\
       when(col("roadname").contains("高速"), lit("highway")).\
       when(col("roadname").contains("国道"), lit("national_highway")).\
       when((col("roadname").contains("街")) & (col("roadname").contains("路")), lit("city")).\
       when((col("roadname").contains("省道")) & (col("roadname").contains("县道")) & (col("roadname").contains("乡道")), lit("others")).otherwise(lit("unknown"))).\
       withColumn("first_day_Of_week", date_sub(next_day(to_date("occurrence_date_time"), 'Mon'), 7)).\
       withColumn("vehicle_speed", when(col("wheel_based_vehicle_speed").isNotNull(), col("wheel_based_vehicle_speed")).otherwise(col("gps_vehicle_speed")))



    wSpec = Window.partitionBy("engine_serial_number", "first_day_of_week").orderBy("occurrence_date_time")

    windowDF = formatRoadDF.filter(formatRoadDF.vehicle_speed > 0).\
        withColumn("lag_distance", lag("high_resolution_total_vehicle_distance").over(wSpec)).\
        withColumn("lag_time", lag("occurrence_date_time").over(wSpec))

    filterDF = windowDF.withColumn("delta_distance", (col("high_resolution_total_vehicle_distance")-col("lag_distance"))/1000).\
        withColumn("delta_time", (col("occurrence_date_time").cast("long")-col("lag_time").cast("long"))/3600).\
        withColumn("ratio", abs(col("delta_distance"))/abs(col("delta_time"))).\
        filter((col("ratio")<1000) | (col("delta_time") != 0))


    resultDF = filterDF.groupBy("engine_serial_number", "first_day_Of_week", "formatted_road").agg(sum(col("delta_distance")).alias("mileage"))

    wSpec2 = Window.partitionBy("engine_serial_number", "first_day_of_week").orderBy("first_day_of_week")
    windowDF2 = resultDF.withColumn("total_mileage", sum("mileage").over(wSpec2)).\
        withColumn("mileage_percentage", (col("mileage").cast("double")/col("total_mileage").cast("double"))*100)

    saveAsTable(windowDF2, "data_label", "road_percentage", hivePath+"road_percentage/")



hivePath = "wasbs://westlake@dldevblob4hdistorage.blob.core.chinacloudapi.cn/westlake_data/"
def saveData(df, db, tbl):
    df.write. \
        mode("overwrite").format("parquet"). \
        save("wasbs://westlake@sa01cndev19909.blob.core.chinacloudapi.cn/test/" + "city_percentage1/")

def saveAsTable(df, db, tbl, hivePath):
    df.write.format("parquet") \
        .mode("overwrite") \
        .option('path', hivePath) \
        .saveAsTable(db + "." + tbl)

if __name__ == "__main__":
    main()