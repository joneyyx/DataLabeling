#-*- coding: UTF-8 -*-
from pyspark.sql.functions import *
from pyspark.sql import SparkSession, Window
from pyspark.sql.types import *
import argparse
import numpy as np
from datetime import datetime, timedelta

spark = SparkSession.builder.appName("Transport_Patterns").enableHiveSupport().getOrCreate()
spark.conf.set("spark.sql.shuffle.partitions", 400)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
spark.conf.set("spark.sql.broadcastTimeout", 36000)
spark.conf.set("spark.rpc.askTimeout", 300)
spark.conf.set("spark.dynamicAllocation.enabled", "true")

def main(sunday):
    data = getHBData(sunday)
    hivePath = "wasbs://aadecdlcnprdspark01@saaa01cnprd19909.blob.core.chinacloudapi.cn/hive/warehouse/de_labeling/"

    bulletinOriginDF = spark.sql("select esn from de_labeling.engine_bulletin").distinct()
    baseDF = data.join(bulletinOriginDF, on = "esn" , how = "left_semi").\
        withColumn("unix_time", unix_timestamp(col("occurrence_date_time_Beijing"))).\
        withColumn("adjusted_vehicle_distance", udf_floorDistance("high_resolution_total_vehicle_distance")).cache() # floor the vehicle distance into 100

    findRouteAttributes(baseDF, hivePath)
    print("finished route attributes")
    findParkProvinceCity(baseDF, hivePath)
    print("finished city and province routes")

    #三张表合一: cw车窝; td运距; cp停靠城市
    cw = spark.table("de_labeling.transport_attributes")
    td = spark.table("de_labeling.transport_distance").drop("c_lgt", "c_lat")
    cp = spark.table("de_labeling.city_map")

    joinDF1  = cw.join(td, ["esn", "calc_date_sunday"], "outer")
    joinDF2  = joinDF1.join(cp, ["esn", "calc_date_sunday"], "outer")
    saveAsTable(joinDF2, "de_labeling", "transport_attributes", hivePath + "transport_attributes", "overwrite")

    """
    1. 判断车窝
    2. 判断线路重复指数
    3. 判断线路稳定指数
    """
def findRouteAttributes(baseDF, hivePath):
    #lgt, lat 按km画格子
    # 选择engine_speed >0 的HB点; 过滤掉没有GPS点的数据
    # 过滤掉中国边界之外的: 最东端-东经135度2分30秒-135.041667, 最西端-东经73度40分-73.666667, 最南端-北纬3度52分-3.866667, 最北端 北纬53度33分-53.55
    # 不直接对baseDF过滤的原因是，计算经过城市的时候可以借助前后补数据的方式来获得city
    hbProcessedDF = baseDF.withColumn("longitude", format_number(col("longitude"), 2)).\
        withColumn("latitude", format_number(col("latitude"),2)).\
        filter(col("engine_speed") > 0).\
        filter((col("longitude").isNotNull() & (col("longitude") <= 135.041667) & (col("latitude") >= 73.666667))).\
        filter((col("latitude") >= 3.866667) & (col("latitude") <= 53.55))
        # withColumn("unix_time", unix_timestamp(col("occurrence_date_time_beijing")))

    wSpec = Window.partitionBy("esn","longitude", "latitude").orderBy("unix_time")
    hbWindowDF =  hbProcessedDF.withColumn("unix_diff", col("unix_time") - lag(col("unix_time"),1,0).over(wSpec))

    hbAggDF = hbWindowDF.groupBy("esn", "calc_date_sunday" ,"longitude", "latitude").agg(
        count(when(hbWindowDF.unix_diff > 100, 1)).alias("count_nbr")
    )

    percentage_esn = hbAggDF.groupBy("esn", "calc_date_sunday").agg(
        sum(when(hbAggDF.count_nbr == 1, col("count_nbr"))).alias("equals_1_count"),
        sum(when(hbAggDF.count_nbr > 1,  col("count_nbr"))).alias("bigger_than_1_count"),
        sum(when(hbAggDF.count_nbr > 3,  col("count_nbr"))).alias("bigger_than_3_count"),
        sum("count_nbr").alias("esn_squares_count"),
        mean("count_nbr").alias("route_repetition_index"),        #线路重复指数
        stddev("count_nbr").alias("route_stability_index"),       #线路稳定程度
        max("count_nbr").alias("count_nbr")           #车窝-->如果gps格点count数相同，则会有多个待选车窝
    )


    #esn + count_nbr -> get GPS points for every 车窝
    #GPS points如果取均值之后会有多位
    getParkingHoustDF = percentage_esn.join(hbAggDF, ["esn", "count_nbr", "calc_date_sunday"], "left").\
        groupBy("esn", "count_nbr", "calc_date_sunday").agg(
        first("equals_1_count").alias("equals_1_count"),
        first("bigger_than_1_count").alias("bigger_than_1_count"),
        first("bigger_than_3_count").alias("bigger_than_3_count"),
        first("esn_squares_count").alias("esn_squares_count"),
        first("route_repetition_index").alias("route_repetition_index"),
        first("route_stability_index").alias("route_stability_index"),
        mean("longitude").alias("c_lgt"),            #车窝
        mean("latitude").alias("c_lat")              #车窝
    ).withColumnRenamed("count_nbr", "vehicle_parking_house").cache()


    resultDF = getParkingHoustDF.withColumn("bigger_than_1_percentage", col("bigger_than_1_count") / col("esn_squares_count")). \
        withColumn("bigger_than_3_percentage", col("bigger_than_3_count") / col("esn_squares_count"))


    saveAsTable(resultDF, "de_labeling", "vehicle_parking_house", hivePath+"vehicle_parking_house", "overwrite")

    # TODO 运距通过getParkingHoustDF里拿到车窝
    vehicleParkHouseDF = getParkingHoustDF.select("esn", "calc_date_sunday", "c_lgt", "c_lat")

    # 停车超过半小时，的第一个gps点作为sites， 要过滤engine_speed =0 的情况，因为会导致很多停靠点, 过滤里程为null
    sitesWindow = Window.partitionBy("adjusted_vehicle_distance", "esn", "calc_date_sunday")
    sites = baseDF.filter(col("engine_speed") > 0).\
        filter(col("adjusted_vehicle_distance").isNotNull()). \
        filter(col("longitude") > 0). \
        filter(col("longitude").isNotNull()).\
        withColumn("staydur", max(col("unix_time")).over(sitesWindow) - min(col("unix_time")).over(sitesWindow)).\
        withColumn("row_number", row_number().over(sitesWindow.orderBy("unix_time"))).\
        filter((col("row_number")  == 1) &  (col("staydur") > 1800))

    #TODO 车窝中心有Null，待排查原因
    transportDistanceBaseDF = sites.join(vehicleParkHouseDF, ["esn", "calc_date_sunday"], "left")

    distanceResultDF  = geodistance(transportDistanceBaseDF, "haversine_dist", "c_lgt", "c_lat", "longitude", "latitude")

    longestDistanceDF = distanceResultDF.groupBy("esn", "calc_date_sunday").agg(
        max("haversine_dist").alias("sites_longest_distance_from_cw"),
        first("c_lgt").alias("c_lgt"),
        first("c_lat").alias("c_lat"),
        (max("high_resolution_total_vehicle_distance")-min("high_resolution_total_vehicle_distance")).alias("weekly_moved_distance")
    )

    saveAsTable(longestDistanceDF, "de_labeling", "transport_distance", hivePath + "transport_distance/", "overwrite")
    # new_sites = sites.withColumn("mileage_diff", col("high_resolution_total_vehicle_distance") - lag(col("high_resolution_total_vehicle_distance")).over(Window.partitionBy("esn", "calc_date_sunday").orderBy("unix_time")))
    # new_sites.join(longestDistanceDF, ["esn", "calc_date_sunday"], "left").\
    #     filter(col("mileage_diff") > 100 * col("harversine_dist"))



    """
    判断经停省市
    """
def findParkProvinceCity(baseDF, hivePath):
    # fill the Null with previous data
    filledNullCityDF = fillNullWithForward(baseDF, ["esn", "calc_date_sunday"], "occurrence_date_time_beijing", "city", "city")
    filledNullCityDF_BACK = fillNullWithBackward(filledNullCityDF, ["esn", "calc_date_sunday"], "occurrence_date_time_beijing", "city", "city")
    filledNullProvinceDF = fillNullWithForward(filledNullCityDF_BACK, ["esn", "calc_date_sunday"], "occurrence_date_time_beijing", "province", "province")
    filledNullProvinceDF_BACK = fillNullWithBackward(filledNullProvinceDF, ["esn", "calc_date_sunday"], "occurrence_date_time_beijing", "province", "province")


    #cleaning: 1. speed  2. city with []

    Municipality = ["北京市", "天津市", "上海市", "重庆市", "澳门特别行政区", "香港特別行政区"]
    # 可以用来去除最后一个字:-1   => expr("substring(province, 1, length(province)-1)"))
    # note the substring start position is 1 based not 0
    cleanDF = filledNullProvinceDF_BACK.withColumn("vehicle_speed", when((col("wheel_based_vehicle_speed") == '0.0') & (col("gps_vehicle_speed") != '0.0'),  col("gps_vehicle_speed")).otherwise(col("wheel_based_vehicle_speed"))).\
        withColumn("city", when(filledNullProvinceDF_BACK["city"].isNull(), lit("Unknown")).otherwise(filledNullProvinceDF_BACK["city"])). \
        withColumn("province", when(filledNullProvinceDF_BACK["province"].isNull(), lit("Unknown")).otherwise(filledNullProvinceDF_BACK["province"])). \
        withColumn("city", when(col("province").isin(Municipality), substring(col("province"), 1, 2)).otherwise(col("city")))
        # withColumn("unix_time", unix_timestamp(col("occurrence_date_time_Beijing")))

    # vehicle speed < 3
    searchParkPointDF = cleanDF.filter(cleanDF.vehicle_speed <= 3).\
        groupBy("esn", "calc_date_sunday", "city", "adjusted_vehicle_distance").\
        agg((max("unix_time") - min("unix_time")).alias("delta_time")).\
        filter(col("delta_time") >= 1800)  #每100米里程组里面时间跨度超过30分钟即为停车点


    #1. 周滑动窗口-停靠频次(in order)
    aggDF2 = searchParkPointDF.groupBy("esn", "city", "calc_date_sunday").count().withColumnRenamed("count", "HB_CMP_1WEEK_STOP_CITY_CNT")
    parkMapDF = aggDF2.groupBy("esn", "calc_date_sunday").agg(collect_list(create_map(col("city"), col("HB_CMP_1WEEK_STOP_CITY_CNT"))).alias("city_park_map"))

    vehicleParkDF = searchParkPointDF.groupBy("esn", "calc_date_sunday").agg(collect_set("city").alias("city_park_set")). \
        withColumn("city_park_set", sort_array(col("city_park_set")))

    cityParkByWeekDF = vehicleParkDF.join(parkMapDF, on=["esn", "calc_date_sunday"])
    saveAsTable(cityParkByWeekDF, "de_labeling", "city_map", hivePath + "city_map/", "overwrite")

    #3.  周滑动窗口-经过频次
    wSpec = Window.partitionBy("esn", "calc_date_sunday").orderBy("occurrence_date_time_beijing")
    windowDF = cleanDF.select("esn", "occurrence_date_time_beijing", "city", "calc_date_sunday").withColumn("city_lag", lead("city").over(wSpec)).\
        withColumn("label_pass_city", when(col("city")==col("city_lag"), lit(0)).otherwise(lit(1))). \
        filter(col("city_lag").isNotNull())

    aggDF3 = windowDF.groupBy("esn", "city", "calc_date_sunday").agg(sum("label_pass_city").alias("HB_CMP_1WEEK_PASS_CITY_CNT"))

    cityPassCountDF = aggDF2.join(aggDF3, on = ["esn", "city", "calc_date_sunday"], how="outer")

    saveAsTable(cityPassCountDF, "de_labeling", "city_frequency", hivePath+"city_frequency/", "overwrite")



######################################################functions: Regular Routes########################################################
def geodistance(df, target_name,lng1, lat1, lng2, lat2):
  result =  df.withColumn("dlon", radians(col(lng1)) - radians(col(lng2))) \
    .withColumn("dlat", radians(col(lat1)) - radians(col(lat2))) \
    .withColumn(target_name, asin(sqrt(sin(col("dlat") / 2) ** 2 + cos(radians(col(lat2)))* cos(radians(col(lat1))) * sin(col("dlon") / 2) ** 2)) * 2 * 3963 * 5280) \
    .drop("dlon", "dlat")
  return result



#######################################################functions : Park province and city################################

# This function is to fill the Null values by the previous and following not null value
def fillNullWithForward(df, partition_columns_list, order_columns, fill_column, target_column):
    wspec = Window.partitionBy(*partition_columns_list).orderBy(order_columns)
    df_new = df.withColumn(target_column, last(fill_column, True).over(wspec))  # True: fill with last not-null
    return df_new

def fillNullWithBackward(df, partition_columns_list, order_columns, fill_column, target_column):
    wspec = Window.partitionBy(*partition_columns_list).orderBy(desc(order_columns))
    df_new = df.withColumn(target_column, last(fill_column, True).over(wspec))  # True: fill with last not-null
    return df_new



#udf->规整distance（规整到100，10199 -> 10100）
def floorDistance(vehicle_Distance):
    if not vehicle_Distance:
        return None
    else:
        return vehicle_Distance if vehicle_Distance % 100 == 0 else vehicle_Distance - vehicle_Distance % 100

udf_floorDistance = udf(floorDistance, IntegerType())
#in SQL:  spark.udf.register("floorDistance_udf", floorDistance_udf, LongType())





#################################################general functions#######################################################
def getHBData(sunday):
    monday = (datetime.strptime(sunday, '%Y%m%d') - timedelta(days= 6)).strftime('%Y%m%d')
    #多取一天，避免HB跨天问题
    last_sunday = (datetime.strptime(sunday, '%Y%m%d') - timedelta(days= 7)).strftime('%Y%m%d')
    #用HB Occurrence_date_time_beijing过滤所选时间段内的数据
    data = spark.sql("select engine_serial_number,occurrence_date_time,longitude,latitude, wheel_based_vehicle_speed, gps_vehicle_speed,  province, city,high_resolution_total_vehicle_distance, engine_speed from pri_tel.tel_hb_labeled where report_date >= '{}' and report_date <= {}".format(last_sunday,sunday))
    processedData =  data.withColumn("occurrence_date_time_Beijing", from_unixtime(unix_timestamp(col("occurrence_date_time")) + 8 *3600 )).\
        withColumn("calc_date", to_date(col("occurrence_date_time_Beijing"))).\
        withColumn("report_date", date_format(col("calc_date"), "yyyyMMdd")). \
        withColumn("calc_date_sunday", to_date(from_unixtime(unix_timestamp(lit(sunday), "yyyyMMdd"), "yyyy-MM-dd"))). \
        filter((col("report_date") >= monday) & (col("report_date") <= sunday)).\
        withColumnRenamed("engine_serial_number", "esn").distinct()
    print("successfully get hb data from {} to {}".format(monday, sunday))
    return processedData





def saveAsTable(df, db, tbl, hivePath, mode):
    df.write.format("parquet") \
        .mode(mode) \
        .option('path', hivePath) \
        .saveAsTable(db + "." + tbl)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--ngrams", help="some useful description.")
    args = parser.parse_args()
    """
    input: Sunday manually
    """
    if args.ngrams:
       sunday  = args.ngrams
       print("input date is >>>>>>>>>>{}".format(sunday))
       main(sunday)
    else:
        """
        input: crontab -> Monday
        output:last Sunday
        """
        today = datetime.now()
        print("current timestamp is : {}".format(today))
        sunday = datetime.strftime(datetime.now() - timedelta(1), '%Y%m%d')
        main(sunday)


