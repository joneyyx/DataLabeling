#-*- coding: UTF-8 -*-
from datetime import timedelta, datetime

from pyspark.sql.functions import *

from pyspark.sql import SparkSession, Window
from pyspark.sql.types import *
from pyspark import *
import argparse
import numpy as np

parser = argparse.ArgumentParser()
parser.add_argument("--ngrams", help="some useful description.")
args = parser.parse_args()


spark = SparkSession.builder.appName("Behavior_Attributes_Weekly").enableHiveSupport().getOrCreate()
spark.conf.set("spark.sql.shuffle.partitions", 400)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
spark.conf.set("spark.sql.broadcastTimeout", 36000)
spark.conf.set("spark.rpc.askTimeout", 300)
spark.conf.set("spark.dynamicAllocation.enabled", "true")



def main(dt):
    print("hello, input date(sunday) is : {}".format(dt))
    hivePath = "wasbs://aadecdlcnprdspark01@saaa01cnprd19909.blob.core.chinacloudapi.cn/hive/warehouse/de_labeling/"

    efpaResultDF = weeklyEFPARun(dt)

    #create a table to save the hisotry of hb ==>daily_last_operation_days
    if spark._jsparkSession.catalog().tableExists('de_labeling', 'hb_operation_days'):
        hbResultDF = weeklyHBRun(dt, hivePath)
    else:    #first run
        operationDateDF = readHBLabeled(dt).\
            withColumn("operation_date", to_date(col("occurrence_date_time_Beijing"))).\
            select("engine_serial_number", "operation_date").distinct().\
            groupBy("engine_serial_number").agg(max("operation_date").alias("operation_date"))

        saveAsTable(operationDateDF, "de_labeling", "hb_operation_days", hivePath+"hb_operation_days/", "overwrite")


    resultJoinDF = hbResultDF.join(efpaResultDF, on= ["esn", "calc_date"], how= "outer").\
        withColumn("efpa_cmp_1week_agingindex", col("hb_cmp_1week_idleincluded_average_speed_km_h") / col("efpa_cmp_1week_fuelvolumetotal_l_100km"))

    # saveAsTable(resultJoinDF, "de_labeling", "resultJoin", hivePath+"resultJoin__temp/")

    bulletinDF = readEngineBulletinDF()
    bulletin_Hb_Efpa = bulletinDF.join(resultJoinDF, on="esn", how="left")

    #####################################add Fault_Code Touch on this dt#####################################################
    fcOriginDF = spark.table("qa_prd_primary.tel_fault_code_detail").select("ENGINE_SERIAL_NUMBER", "OCCURRENCE_DATE_TIME", "FAULT_CODE").\
        withColumn("calc_date", to_date("OCCURRENCE_DATE_TIME")).\
        withColumn("report_date", date_format(col("calc_date"), "yyyyMMdd")).\
        filter(col("report_date") == dt).\
        groupBy("ENGINE_SERIAL_NUMBER", "calc_date", "FAULT_CODE").agg(count("ENGINE_SERIAL_NUMBER").alias("count_nbr")).cache()


    # spark3.0.0=> map_from_entries
    # <spark 2.4.0  use the following udf
    def map_array(col):
        return dict(col)

    array_map = udf(map_array, MapType(StringType(), StringType()))

    fcProcessedDF = fcOriginDF.withColumn("struct_type", struct("FAULT_CODE","count_nbr")).\
        withColumn("map_type", create_map("FAULT_CODE", "count_nbr")).\
        groupBy("ENGINE_SERIAL_NUMBER", "calc_date").agg(
        collect_list(col("struct_type")).alias("key_value")
        ).\
        withColumn("tel_cmp_1week_fc_cnt", array_map(col("key_value"))).\
        drop("key_value").\
        withColumnRenamed("ENGINE_SERIAL_NUMBER", "esn")

    bulletin_Hb_Efpa_Fc  = bulletin_Hb_Efpa.join(fcProcessedDF, on=["esn", "calc_date"], how="left")


    #############################################add claim touch on this dt########################################
    claimOriginDF = spark.table("qa_prd_primary.rel_engine_claim_detail").select("ESN_NEW", "CLAIM_NUMBER", "FAILURE_DATE").\
        withColumnRenamed("ESN_NEW", "esn").\
        withColumn("calc_date", to_date("FAILURE_DATE")).\
        withColumn("report_date", date_format(col("calc_date"), "yyyyMMdd")).\
        filter(col("report_date") == dt)

    claimProcessedDF = claimOriginDF.groupBy("esn", "calc_date").agg(count("CLAIM_NUMBER").alias("tel_cmp_1week_claim_cnt"))

    bulletin_Hb_Efpa_Fc_Claim = bulletin_Hb_Efpa_Fc.join(claimProcessedDF, on=["esn", "calc_date"], how="left")





def weeklyHBRun(dt, hivePath):
    #1. read basic_attributes===>make sure basic_attributes have not duplicates
    vehicleBasicAttOriginDF = readBasicAttributes().dropDuplicates(["esn"])

    #2. read data on specific date
    #input: Sunday date (20201101)
    #return the efpa data in this range
    hbOriginDF = readHBLabeled(dt)

    #3. join 1 & 2 and get the base table===>only calculate the ESNs that in basic_attributes df
    baseOriginDF = hbOriginDF.join(vehicleBasicAttOriginDF, hbOriginDF["engine_serial_number"] == vehicleBasicAttOriginDF["esn"], "left_semi")

    #4. Join with operation_date df to calculate the daily last date
    operationDateDF = spark.table("de_labeling.hb_operation_days")
    joinWithOperationDateDF = baseOriginDF.join(operationDateDF, on=["engine_serial_number"], how="outer")

    #5. update operation_date df
    saveBacktoOperationDF = joinWithOperationDateDF.select("engine_serial_number", "calc_date", "operation_date").\
        withColumn("operation_date", when(col("calc_date").isNotNull(), col("calc_date")).otherwise(col("operation_date"))).\
        select("engine_serial_number", "operation_date").distinct().\
        groupBy("engine_serial_number").agg(max("operation_date").alias("operation_date"))

    saveAsTable(saveBacktoOperationDF, "de_labeling", "hb_operation_days", hivePath + "hb_operation_days/", "overwrite")

    #6. process the table to do the following tables
    hbProcessedDF = joinWithOperationDateDF.filter(joinWithOperationDateDF.calc_date.isNotNull()). \
        withColumn("wheel_based_vehicle_speed", when((col("wheel_based_vehicle_speed") == '0.0') & (col("gps_vehicle_speed") != '0.0'),  col("gps_vehicle_speed")).otherwise(col("wheel_based_vehicle_speed"))).\
        withColumn("TEMP_last_operation_days",when(col("operation_date").isNotNull(), datediff(col("calc_date"), col("operation_date"))).otherwise(lit(0))).cache()


##############################################################################################################
    # TODO 用HB来计算里程油耗的算法
    window_mileage_fuel = Window.partitionBy("engine_serial_number").orderBy("unix_time")

    calculateSpeedDF = hbProcessedDF.withColumn("unix_time", unix_timestamp(col("occurrence_date_time_beijing"))).\
        withColumn("oct_beijing_diff", col("unix_time") - lag(col("unix_time"), 1, 0).over(window_mileage_fuel.orderBy("unix_time"))).\
        filter(col("oct_beijing_diff") < 120).\
        withColumn("max_mileage", max(col("high_resolution_total_vehicle_distance")).over(window_mileage_fuel)).\
        withColumn("min_mileage", min(col("high_resolution_total_vehicle_distance")).over(window_mileage_fuel)).\
        withColumn("median_mileage", expr('percentile_approx(high_resolution_total_vehicle_distance, 0.5)').over(window_mileage_fuel)).\
        withColumn("mileage_diff_1", col("max_mileage") - col("median_mileage")).\
        withColumn("mileage_diff_2", col("median_mileage") - col("min_mileage"))


    def findLabel(baseCol, maxCol, minCol, medianCol):
        if baseCol != None:
            if maxCol != None and baseCol == maxCol:
                return "max_label"
            elif minCol != None and baseCol == minCol:
                return "min_label"
            elif medianCol != None and baseCol == medianCol:
                return "median_label"
            else:
                return None
        else:
            return None

    udf_findLabel = udf(findLabel, StringType())

    test = calculateSpeedDF.withColumn("label", udf_findLabel(col("high_resolution_total_vehicle_distance"), col("max_mileage"), col("min_mileage"), col("median_mileage")))



##############################################################################################################


    hbAggedDF = hbProcessedDF.groupBy("engine_serial_number", "calc_date_sunday").agg(
        max(col("TEMP_last_operation_days")).alias("1week_last_operation_days"),
        count(when((col("calc_time") > "00:00:00") & (col("calc_time") <= "06:00:00"), 1)).alias("tel_cmp_1week_6oclock_engruntime_h"),
        count(when((col("calc_time") > "06:00:00") & (col("calc_time") <= "12:00:00"), 1)).alias("tel_cmp_1week_12oclock_engruntime_h"),
        count(when((col("calc_time") > "12:00:00") & (col("calc_time") <= "18:00:00"), 1)).alias("tel_cmp_1week_18oclock_engruntime_h"),
        count(when((col("calc_time") > "18:00:00") & (col("calc_time") <= "24:00:00"), 1)).alias("tel_cmp_1week_24oclock_engruntime_h"),
        avg(when((col("wheel_based_vehicle_speed") > 0), col("wheel_based_vehicle_speed"))).alias("hb_cmp_1week_average_speed_km_h"),
        count(when((col("wheel_based_vehicle_speed") > 0), 1)).alias("hb_cmp_1week_average_speed_hbcount"),
        avg(col("wheel_based_vehicle_speed")).alias("hb_cmp_1week_idleincluded_average_speed_km_h"),
        count(when(col("engine_speed") > 0, 1)).alias("hb_cmp_1week_idleincluded_average_speed_hbcount"),
        count(when((col("landscape") == "Plain"), 1)).alias("hb_cmp_1week_plain_count"),
        count(when((col("landscape") == "Hills"), 1)).alias("hb_cmp_1week_hills_count"),
        count(when((col("landscape") == "Mountain_Area"), 1)).alias("hb_cmp_1week_mountains_count"),
        count(when((col("landscape") == "Plateau"), 1)).alias("hb_cmp_1week_plateau_count"),
        count(when((col("landscape") == "High_Plateau"), 1)).alias("hb_cmp_1week_highplateau_count"),
        count(col("landscape")).alias("hb_cmp_1week_landscape_hbcount")
    ).withColumnRenamed("engine_serial_number", "esn")

    return hbAggedDF



def weeklyEFPARun(dt):
    #1. read basic attributes
    # array_mean = udf(lambda x: float(np.mean(x)), FloatType())
    def array_mean(col):
        res = 0.0
        if col:
            for number in col:
                res += float(number)
                return res
        else:
            return None

    array_mean = udf(array_mean, FloatType())

    #!!!!!distinct to basic_attributes table
    vehicleBasicAttOriginDF = readBasicAttributes().\
        withColumn("mean_vehicl_curb_weight_kg", array_mean("vehicle_curb_weight_kg")).\
        drop("vehicle_curb_weight_kg").distinct()


    #2. read data on specific date
    #input: Sunday date (20201101)
    #return the efpa data in this range
    efpaOriginDF = readEfpaDF(dt)

    #3. join 1 & 2 and get the base table for calculation
    #!!用right不用left_semi是因为vehicle_basic_attributes里面有重复值。通过业务解决该问题
    baseOriginDF = efpaOriginDF.join(vehicleBasicAttOriginDF, on= ["esn"], how="right").\
        filter(col("calc_date").isNotNull())

    """
    pyspark 2.4 -> calculate sum
    withColumn("vehSpeedBin_total", expr('aggregate(col("vehspeedbin").split(","), 0, (acc, x) -> acc + x)'))
    """
    efpaProcessedDF = baseOriginDF.withColumn("TEMP_tel_cmp_1week_windresistancework_kwh", udf_calculateKWH_Molecules(col("VSPDAve"), col("drivetime"), col("TotalAirResistenceWork"))).\
        withColumn("TEMP_denominators_tel_cmp_1week_windresistancework_kw", udf_calculateKWH_denominators(col("VSPDave"), col("drivetime"), col("TotalAirResistenceWork"))).\
        withColumn("TEMP_tel_cmp_1week_TotalWorkLossFromBrake_kwh", udf_calculateKWH_Molecules(col("VSPDAve"), col("drivetime"), col("TotalWorkLossFromBrake"))). \
        withColumn("TEMP_denominators_tel_cmp_1week_TotalWorkLossFromBrake_kw", udf_calculateKWH_denominators(col("VSPDave"), col("drivetime"), col("TotalWorkLossFromBrake"))). \
        withColumn("TEMP_tel_cmp_1week_TotalWorkLossFromSevereBrake_kwh", udf_calculateKWH_Molecules(col("VSPDAve"), col("drivetime"), col("TotalWorkLossFromSevereBrake"))).\
        withColumn("TEMP_denominators_tel_cmp_1week_TotalWorkLossFromSevereBrake_kw", udf_calculateKWH_denominators(col("VSPDAve"), col("drivetime"), col("TotalWorkLossFromSevereBrake"))).\
        withColumn("TEMP_ESPD", col("ESPDAve") * col("EngRunTime")).\
        withColumn("vehspeedbin", regexp_replace("vehspeedbin", "[\\[\\]]", "")).\
        withColumn("VehSpeedBin_0", split(col("vehspeedbin"), ",").getItem(0) / 3600).\
        withColumn("VehSpeedBin_1", split(col("vehspeedbin"), ",").getItem(1) / 3600).\
        withColumn("VehSpeedBin_2", split(col("vehspeedbin"), ",").getItem(2) / 3600).\
        withColumn("VehSpeedBin_3", split(col("vehspeedbin"), ",").getItem(3) / 3600).\
        withColumn("VehSpeedBin_4", split(col("vehspeedbin"), ",").getItem(4) / 3600).\
        withColumn("VehSpeedBin_5", split(col("vehspeedbin"), ",").getItem(5) / 3600).\
        withColumn("VehSpeedBin_6", split(col("vehspeedbin"), ",").getItem(6) / 3600).\
        withColumn("VehSpeedBin_7", split(col("vehspeedbin"), ",").getItem(7) / 3600).\
        withColumn("TEMP_efpa_cmp_1week_cargoturnover_tkm", col("MMEmean") / 1000 * col("VSPDave") * col("drivetime") /3600 ).\
        withColumn("esn_trip_engine_run_time_hour", col("engruntime") / 3600 ).\
        withColumn("filter_condition_of_empty_mileage", (col("MMEmean")-col("mean_vehicl_curb_weight_kg"))/col("MMEmean")).\
        withColumn("filter_condition_of_mileage", col("VSPDave") * col("drivetime") / 3600)

    efpaAggedDF  = efpaProcessedDF.groupBy("esn", "calc_date_sunday").\
        agg(sum("TEMP_tel_cmp_1week_windresistancework_kwh").alias("tel_cmp_1week_windresistancework_kwh"),
            sum("TEMP_denominators_tel_cmp_1week_windresistancework_kw").alias("tel_cmp_1week_windresistancework_kw"),
            sum("TEMP_tel_cmp_1week_TotalWorkLossFromBrake_kwh").alias("tel_cmp_1week_TotalWorkLossFromBrake_kwh"),
            sum("TEMP_denominators_tel_cmp_1week_TotalWorkLossFromBrake_kw").alias("tel_cmp_1week_TotalWorkLossFromBrake_kw"),
            sum("TEMP_tel_cmp_1week_TotalWorkLossFromSevereBrake_kwh").alias("tel_cmp_1week_TotalWorkLossFromSevereBrake_kwh"),
            sum("TEMP_denominators_tel_cmp_1week_TotalWorkLossFromSevereBrake_kw").alias("tel_cmp_1week_TotalWorkLossFromSevereBrake_kw"),
            sum("TEMP_ESPD").alias("agg_ESPD"),
            sum("EngRunTime").alias("agg_engine_runtime"),
            sum("VehSpeedBin_0").alias("efpa_cmp_1week_speed_2km/h_h"),
            sum("VehSpeedBin_1").alias("efpa_cmp_1week_speed_15km/h_h"),
            sum("VehSpeedBin_2").alias("efpa_cmp_1week_speed_30km/h_h"),
            sum("VehSpeedBin_3").alias("efpa_cmp_1week_speed_45km/h_h"),
            sum("VehSpeedBin_4").alias("efpa_cmp_1week_speed_60km/h_h"),
            sum("VehSpeedBin_5").alias("efpa_cmp_1week_speed_75km/h_h"),
            sum("VehSpeedBin_6").alias("efpa_cmp_1week_speed_90km/h_h"),
            sum("VehSpeedBin_7").alias("efpa_cmp_1week_speed_90pluskm/h_h"),
            sum("TEMP_efpa_cmp_1week_cargoturnover_tkm").alias("efpa_cmp_1week_cargoturnover_tkm"),
            sum("esn_trip_engine_run_time_hour").alias("esn_engine_run_time_hour"),
            sum(when(col("filter_condition_of_empty_mileage") < 0.05, col("VSPDave") * col("drivetime") / 3600)).alias("efpa_cmp_1week_empty_mileage_km"),
            sum(when(col("filter_condition_of_empty_mileage") >= 0.05, (col("MMEmean")-col("mean_vehicl_curb_weight_kg")) * col("VSPDave") * col("drivetime") / 3600)).alias("Molecules_1week_averageload"),
            sum(when(col("filter_condition_of_empty_mileage") >= 0.05, col("VSPDave") * col("drivetime") / 3600)).alias("denominators_1week_averageload"),
            sum(when((col("fuelvolumetotal") != 0) & (col("DEFVolumeTotal") != 0),col("fuelvolumetotal"))).alias("efpa_cmp_1week_urea_fuel_pct_l"),
            sum(when((col("fuelvolumetotal") != 0) & (col("DEFVolumeTotal") != 0), col("DEFVolumeTotal"))).alias("efpa_cmp_1week_urea_fuel_volumn"),
            sum(when((col("filter_condition_of_mileage") > 5) & (col("VSPDave") != 0) & (col("drivetime") != 0) & (col("fuelvolumetotal") != 0), col("fuelvolumetotal"))).alias("Molecules_cmp_1week_fuelvolumetotal_l_100km"),
            sum(when((col("filter_condition_of_mileage") > 5) & (col("VSPDave") != 0) & (col("drivetime") != 0) & (col("fuelvolumetotal") != 0), col("VSPDave") * col("drivetime") / 3600)).alias("efpa_cmp_1week_fuelvolumetotal_l_100km_km"),
            sum(col("engruntime")/3600).alias("efpa_cmp_1week_engruntime_h"),
            sum(when((col("filter_condition_of_empty_mileage") >= 0.05) & (col("MMEmean") != 0) & (col("VSPDave") != 0) & (col("drivetime") != 0) & (col("fuelvolumetotal") != 0), col("fuelvolumetotal"))).alias("efpa_cmp_1week_freighttransportationefficiency_fuel_l"),
            sum(when((col("filter_condition_of_empty_mileage") >= 0.05) & (col("MMEmean") != 0) & (col("VSPDave") != 0) & (col("drivetime") != 0) & (col("fuelvolumetotal") != 0), (col("MMEmean")-col("mean_vehicl_curb_weight_kg")) / 1000 * col("VSPDave") * col("drivetime") * 3600)).alias("Molecules_efpa_cmp_1week_freighttransportationefficiency_tkm_l"),
            )

    efpaProcessAggedDF = efpaAggedDF.withColumn("tel_cmp_1week_average_rotatingspeed_rpm", col("agg_ESPD")/col("agg_engine_runtime")).\
        withColumn("efpa_cmp_1week_speed_total_h", col("efpa_cmp_1week_speed_2km/h_h") + col("efpa_cmp_1week_speed_15km/h_h") + col("efpa_cmp_1week_speed_30km/h_h") + col("efpa_cmp_1week_speed_45km/h_h") + col("efpa_cmp_1week_speed_60km/h_h") + col("efpa_cmp_1week_speed_75km/h_h") + col("efpa_cmp_1week_speed_90km/h_h") + col("efpa_cmp_1week_speed_90pluskm/h_h")).\
        withColumn("efpa_cmp_1week_operating_pct", col("esn_engine_run_time_hour") / 24).\
        withColumn("efpa_cmp_1week_averageload_kg", col("Molecules_1week_averageload") / col("denominators_1week_averageload")).\
        withColumn("efpa_cmp_1week_fuelvolumetotal_l_100km", col("Molecules_cmp_1week_fuelvolumetotal_l_100km") *100 / col("efpa_cmp_1week_fuelvolumetotal_l_100km_km")).\
        withColumn("efpa_cmp_1week_urea_fuel_pct", col("efpa_cmp_1week_urea_fuel_volumn") / col("efpa_cmp_1week_urea_fuel_pct_l")).\
        withColumn("efpa_cmp_1week_freighttransportationefficiency_tkm_l", col("Molecules_efpa_cmp_1week_freighttransportationefficiency_tkm_l") / col("efpa_cmp_1week_freighttransportationefficiency_fuel_l")).\
        withColumn("tel_cmp_1week_vin_active_index", col("efpa_cmp_1week_engruntime_h") / 12)

    efpaResultDF = efpaProcessAggedDF.select("esn",
        "calc_date",
        "tel_cmp_1week_windresistancework_kwh",
        "tel_cmp_1week_windresistancework_kw",
        "tel_cmp_1week_TotalWorkLossFromBrake_kwh",
        "tel_cmp_1week_TotalWorkLossFromBrake_kw",
        "tel_cmp_1week_TotalWorkLossFromSevereBrake_kwh",
        "tel_cmp_1week_TotalWorkLossFromSevereBrake_kw",
        "tel_cmp_1week_average_rotatingspeed_rpm",
        "efpa_cmp_1week_speed_2km/h_h",
        "efpa_cmp_1week_speed_15km/h_h",
        "efpa_cmp_1week_speed_30km/h_h",
        "efpa_cmp_1week_speed_45km/h_h",
        "efpa_cmp_1week_speed_60km/h_h",
        "efpa_cmp_1week_speed_75km/h_h",
        "efpa_cmp_1week_speed_90km/h_h",
        "efpa_cmp_1week_speed_90pluskm/h_h",
        "efpa_cmp_1week_speed_total_h",
        "efpa_cmp_1week_cargoturnover_tkm",
        "efpa_cmp_1week_operating_pct",
        "efpa_cmp_1week_empty_mileage_km",
        "efpa_cmp_1week_averageload_kg",
        "efpa_cmp_1week_fuelvolumetotal_l_100km",
        "efpa_cmp_1week_fuelvolumetotal_l_100km_km",
        "efpa_cmp_1week_urea_fuel_pct",
        "efpa_cmp_1week_urea_fuel_pct_l",
        "efpa_cmp_1week_engruntime_h",
        "efpa_cmp_1week_freighttransportationefficiency_tkm_l",
        "efpa_cmp_1week_freighttransportationefficiency_fuel_l")

    return efpaResultDF








#################################################EFPA Related Functions#########################################
#read EFPA data
def readEfpaDF(dt):
    #input df is Sunday: 20201101
    #create a new column : calc_date_sunday which is used for calculation in weekly
    monday = (datetime.strptime(dt, '%Y%m%d') - timedelta(days=6)).strftime('%Y%m%d')
    df = spark.sql("select esn, report_date, espdave, vspdave, engruntime, drivetime, totalairresistencework, totalworklossfrombrake, totalworklossfromseverebrake, vehspeedbin, mmemean, fuelvolumetotal, defvolumetotal from pri_tel.tel_efpa where report_date >= '{}' and report_date <= '{}'".format(monday,dt))
    processedData = df.filter(df.engruntime >= 600).\
        withColumn("calc_date", to_date(from_unixtime(unix_timestamp(col("report_date"), "yyyyMMdd"), "yyyy-MM-dd"))). \
        withColumn("calc_date_sunday", to_date(from_unixtime(unix_timestamp(lit(dt), "yyyyMMdd"), "yyyy-MM-dd")))
    print("successfully load EFPA data from {} to {}".format(monday, dt))
    return processedData


def calculateKWH_Molecules(colA, colB, colC):
    #colA = VSPDAve, colB = drivetime
    #colC != 0是因为理论上，做工存在等于0的情况
    if colA != None and colA != 0 and colB != None and colB != 0 and colC != None:
        res = colC * 0.000000277777777778
        return res
    else:
        return None


# use udf on DataFrame
udf_calculateKWH_Molecules = udf(calculateKWH_Molecules, StringType())


def calculateKWH_denominators(colA, colB, colC):
    #colA = VSPDAve, colB = drivetime
    # colC != 0是因为理论上，做工存在等于0的情况
    if colA != None and colA != 0 and colB != None and colB != 0 and colC != None:
        res =  colA * colB / 3600
        return res
    else:
        return None


# use udf on DataFrame
udf_calculateKWH_denominators = udf(calculateKWH_denominators, StringType())



################################################read HB data##############################################
#read hb daily data, HB has duplicates data-> distinct()
def readHBLabeled(dt):
    # input df is Sunday: 20201101
    monday = (datetime.strptime(dt, '%Y%m%d') - timedelta(days=6)).strftime('%Y%m%d')
    # 多取一天，避免HB跨天问题
    last_sunday = (datetime.strptime(dt, '%Y%m%d') - timedelta(days=7)).strftime('%Y%m%d')
    df = spark.sql("select engine_serial_number, occurrence_date_time, report_date, engine_speed, high_resolution_total_vehicle_distance ,wheel_based_vehicle_speed, gps_vehicle_speed, landscape, roadname, highway_code from pri_tel.tel_hb_labeled where report_date >= '{}' and report_date <= '{}'".format(last_sunday, dt))
    #convert from UTC to Beijing Time, filter HB from Monday to Sunday
    processedData = df.withColumn("occurrence_date_time_Beijing", from_unixtime(unix_timestamp(col("occurrence_date_time")) + 8 * 3600)). \
        withColumn("report_date", date_format(to_date(col("occurrence_date_time_Beijing")), "yyyyMMdd")). \
        withColumn("calc_time", date_format(col("occurrence_date_time_Beijing"), "HH:mm:ss")). \
        withColumn("calc_date", to_date(col("occurrence_date_time_Beijing"))). \
        withColumn("calc_date_sunday", to_date(from_unixtime(unix_timestamp(lit(dt), "yyyyMMdd"), "yyyy-MM-dd"))).\
        filter((col("report_date") <= dt) & (col("report_date") >= monday)). \
        filter(col("engine_speed") > 0).\
        drop("occurrence_date_time").distinct()
    return processedData




#################################################basic_attributes Related Functions########################
#read labeling_basic_attributes
def readBasicAttributes():
    df = spark.table("de_labeling.labeling_basic_attributes").\
        select("esn", "vehicle_curb_weight_kg")
    return df


#read Bulletin data
def readEngineBulletinDF():
    df  = spark.table("de_labeling.engine_bulletin").\
        select("esn", "engine_type", "engine_manufacturer_name", "fuel_injection_pump_model_and_manufacturing_company",
               "common_rail_tube_model_and_manufacturing_company", "injector_model_and_manufacturing_company",
               "turbo_model_and_manufacturing_company", "crankcase_emission_ctrl_device_model_and_manufacturing_company",
               "ecu_type_and_manufacturing_company", "doc_model_and_manufacturing_company","scr_model_and_manufacturing_company",
               "dpf_model_and_manufacturing_company", "exhaust_orientation", "vehicle_type", "vehicle_classification")
    return df



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
    eg. 20201122
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
