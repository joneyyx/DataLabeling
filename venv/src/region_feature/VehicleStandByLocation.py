#-*- coding: UTF-8 -*-
from pyspark import *
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName("data_Label").enableHiveSupport().getOrCreate()
spark.conf.set("spark.sql.shuffle.partitions", 400)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
spark.conf.set("spark.sql.broadcastTimeout", 36000)
spark.conf.set("spark.rpc.askTimeout", 300)
spark.conf.set("spark.dynamicAllocation.enabled", "true")


def main():
    data = spark.sql("select engine_serial_number, occurrence_date_time, wheel_based_vehicle_speed, gps_vehicle_speed, province, city, high_resolution_total_vehicle_distance, latitude, longitude from data_label.tel_hb_labeled where report_date >= '20200701' and report_date <= '20200731'")

    # fill the Null with previous data
    filledNullCityDF = fillNullWithForward(data, ["engine_serial_number"], "occurrence_date_time", "city", "city")
    filledNullProvinceDF = fillNullWithForward(filledNullCityDF, ["engine_serial_number"], "occurrence_date_time", "province", "province")

    """
    cleaning: 1. speed  2. city with []
    add column: date_time
    """

    Municipality = ["北京市", "天津市", "上海市", "重庆市", "澳门特别行政区", "香港特別行政区"]
    # 可以用来去除最后一个字:-1   => expr("substring(province, 1, length(province)-1)"))
    # note the substring start position is 1 based not 0
    cleanDF = filledNullProvinceDF.withColumn("vehicle_speed", coalesce(filledNullProvinceDF["wheel_based_vehicle_speed"], filledNullProvinceDF["gps_vehicle_speed"])).\
      withColumn("date_time", to_date("occurrence_date_time")). \
      withColumn("city", when(filledNullProvinceDF["city"].isNull(), lit("Unknown")).otherwise(filledNullProvinceDF["city"])). \
      withColumn("province", when(filledNullProvinceDF["province"].isNull(), lit("Unknown")).otherwise(filledNullProvinceDF["province"])). \
      withColumn("city", when(col("province").isin(Municipality), substring(col("province"), 1, 2)).otherwise(col("city"))).\
      withColumn("first_day_Of_week", date_sub(next_day(col("Date_Time"), 'Mon'), 7))



    # vehicle speed < 3
    # floor the vehicle distance into 100
    adjustTimeDF = cleanDF.filter(cleanDF.vehicle_speed <=3).withColumn("adjusted_vehicle_distance", floorDistance_udf("high_resolution_total_vehicle_distance")).\
     orderBy("occurrence_date_time")

    # calculate the parking points for each ESN by day and city
    aggDF = adjustTimeDF.groupBy("engine_serial_number", "Date_Time", "city", "adjusted_vehicle_distance").\
        agg(max("occurrence_date_time").alias("max_time"), min("occurrence_date_time").alias("min_time"))

    searchParkPointDF = aggDF.withColumn("delta_time", (unix_timestamp("max_time")-unix_timestamp("min_time"))/60). \
        withColumn("first_day_Of_week", date_sub(next_day(col("Date_Time"), 'Mon'), 7)).\
        filter(col("delta_time")>=30)

    #停车点需要添加第一个点的里程&GPS

    # 周滑动窗口-停靠频次(in order)
    aggDF2 = searchParkPointDF.groupBy("engine_serial_number", "city", "first_day_Of_week").count().withColumnRenamed("count", "weekly_park_count")
    parkMapDF = aggDF2.groupBy("engine_serial_number", "first_day_Of_week").agg(collect_list(create_map(col("city"), col("weekly_park_count"))).alias("city_park_map"))



    vehicleParkDF = searchParkPointDF.groupBy("engine_serial_number", "first_day_Of_week").agg(collect_set("city").alias("city_park_set")).\
        withColumn("city_park_set", sort_array(col("city_park_set")))

    cityParkByWeekDF = vehicleParkDF.join(parkMapDF, on=["engine_serial_number", "first_day_of_week"])

    saveAsTable(cityParkByWeekDF, "data_label", "city_map", hivePath + "city_map/")



    # 周滑动窗口-经过频次
    # cleanDF

    # Window
    wSpec = Window.partitionBy("engine_serial_number", "first_day_Of_week").orderBy("occurrence_date_time")
    windowDF = cleanDF.select("engine_serial_number", "occurrence_date_time", "city", "first_day_Of_week").withColumn("city_lag", lead("city").over(wSpec)).\
        withColumn("label_pass_city", when(col("city")==col("city_lag"), lit(0)).otherwise(lit(1))). \
        filter(col("city_lag").isNotNull())

    aggDF3 = windowDF.groupBy("engine_serial_number", "city", "first_day_Of_week").agg(sum("label_pass_city").alias("city_pass_by_count"))
        # withColumn("city_pass_by_count", when(col("city_pass_by_count")==0, lit(1)).otherwise(col("city_pass_by_count")))
    # cityPassBy = aggDF3.filter(aggDF3.city_pass_by_count != 0).groupBy("engine_serial_number", "first_day_Of_week").agg(collect_list(create_map(col("city"), col("city_pass_by_count"))).alias("city_pass_map"))


    joinDF = aggDF2.join(aggDF3, on = ["engine_serial_number", "city", "first_day_Of_week"], how="outer")

    saveAsTable(joinDF, "data_label", "city_frequency", hivePath+"city_frequency/")


"""
This function is to fill the Null values by the previous not null value

"""
def fillNullWithForward(df, partition_columns_list, order_columns, fill_column, target_column):
  wspec = Window.partitionBy(*partition_columns_list).orderBy(order_columns)
  df_new = df.withColumn(target_column, last(fill_column, True).over(wspec)) #True: fill with last not-null
  return df_new


"""
udf->规整distance（规整到100，10199 -> 10100）
"""
@udf("bigint")
def floorDistance_udf(vehicle_Distance):
    if not vehicle_Distance:
        return None
    else:
        return vehicle_Distance if vehicle_Distance % 100 == 0 else vehicle_Distance - vehicle_Distance % 100
# spark.udf.register("floorDistance_udf", floorDistance_udf, LongType())

"""
udf to combine the columns into a map(k,v)
"""
def combineToMap_udf(colA, colB):
    return {colA : colB}

spark.udf.register("combineToMap_udf", combineToMap_udf, MapType(StringType(), StringType()))

hivePath="wasbs://westlake@dldevblob4hdistorage.blob.core.chinacloudapi.cn/westlake_data/data_label/"
def saveAsTable(df, db, tbl, hivePath):
    df.write.format("parquet") \
        .mode("overwrite") \
        .option('path', hivePath) \
        .saveAsTable(db + "." + tbl)

if __name__ == "__main__":
    main()



