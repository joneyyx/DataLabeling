#-*- coding: UTF-8 -*-
from pyspark.sql.functions import *
from pyspark.sql import SparkSession, Window
from pyspark.sql.types import *
from pyspark import *

spark = SparkSession.builder.appName("Basic_Attributes").enableHiveSupport().getOrCreate()
spark.conf.set("spark.sql.shuffle.partitions", 400)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
spark.conf.set("spark.sql.broadcastTimeout", 36000)
spark.conf.set("spark.rpc.askTimeout", 300)
spark.conf.set("spark.dynamicAllocation.enabled", "true")

def main():
    print("hello")

    # +------------+
    # | engine_plant |
    # +------------+
    # | 中国重型汽车集团有限公司 |
    # | 北汽福田汽车股份有限公司 |
    # | null |
    # | BFCEC |
    # | ACPL |
    # | 广西玉柴机器股份有限公司 |
    # | DCEC |
    # +------------+

    engineBulletinOriginDF = readEngineBulletinDF().\
        filter(col("engine_type").isNotNull()).\
        withColumn("engine_platform", regexp_extract(col("engine_type"), '(\w+\.?[0-9]?)(NS6)(B)(\d+)', 1)).\
        withColumn("emission_level", regexp_extract(col("engine_type"), '(\w+\.?[0-9]?)(NS6)(B)(\d+)', 2)).\
        withColumn("engine_horsepower", regexp_extract(col("engine_type"), '(\w+\.?[0-9]?)(NS6)(B)(\d+)', 4)). \
        withColumn("engine_plant", when(col("engine_manufacturer_name").like("%福田康明斯%"), lit("BFCEC")). \
            otherwise(when(col("engine_manufacturer_name").like("%东风康明斯%"), lit("DCEC")). \
            otherwise(when(col("engine_manufacturer_name").like("%西安康明斯%"), lit("XCEC")). \
            otherwise(when(col("engine_manufacturer_name").like("%广西康明斯%"), lit("GCIC")). \
            otherwise(when(col("engine_manufacturer_name").like("%重庆康明斯%"), lit("CCEC")). \
            otherwise(when(col("engine_manufacturer_name").like("%安徽康明斯%"), lit("ACPL")). \
            otherwise(col("engine_manufacturer_name")))))))). \
        withColumn("engine_displacement", regexp_extract(col("engine_platform"), '(\w)(\d.?\d?)', 2)). \
        withColumn("engine_fuelpump_model", split(col("fuel_injection_pump_model_and_manufacturing_company"), "/").getItem(0)). \
        withColumn("engine_fuelpump_supplier", split(col("fuel_injection_pump_model_and_manufacturing_company"), "/").getItem(1)). \
        withColumn("engine_fuelrail_model", split(col("common_rail_tube_model_and_manufacturing_company"), "/").getItem(0)). \
        withColumn("engine_fuelrail_supplier", split(col("common_rail_tube_model_and_manufacturing_company"), "/").getItem(1)). \
        withColumn("engine_injector_model", split(col("injector_model_and_manufacturing_company"), "/").getItem(0)). \
        withColumn("engine_injector_supplier", split(col("injector_model_and_manufacturing_company"), "/").getItem(1)). \
        withColumn("engine_turbocharger_model", split(col("turbo_model_and_manufacturing_company"), "/").getItem(0)). \
        withColumn("engine_turbocharger_supplier", split(col("turbo_model_and_manufacturing_company"), "/").getItem(1)). \
        withColumn("engine_crankcase_breathvalve", split(col("crankcase_emission_ctrl_device_model_and_manufacturing_company"), "/").getItem(0)). \
        withColumn("engine_crankcase_breathvalve_supplier", split(col("crankcase_emission_ctrl_device_model_and_manufacturing_company"), "/").getItem(1)). \
        withColumn("engine_ecm_model", split(col("ecu_type_and_manufacturing_company"), "/").getItem(0)). \
        withColumn("engine_ecm_supplier", split(col("ecu_type_and_manufacturing_company"), "/").getItem(1)). \
        withColumn("engine_doc_model", split(col("doc_model_and_manufacturing_company"), "/").getItem(0)). \
        withColumn("engine_doc_supplier", split(col("doc_model_and_manufacturing_company"), "/").getItem(1)). \
        withColumn("engine_scr_model", split(col("scr_model_and_manufacturing_company"), "/").getItem(0)). \
        withColumn("engine_scr_supplier", split(col("scr_model_and_manufacturing_company"), "/").getItem(1)). \
        withColumn("engine_dpf_model", split(col("dpf_model_and_manufacturing_company"), "/").getItem(0)). \
        withColumn("engine_dpf_supplier", split(col("dpf_model_and_manufacturing_company"), "/").getItem(1)). \
        drop("fuel_injection_pump_model_and_manufacturing_company"). \
        drop("common_rail_tube_model_and_manufacturing_company"). \
        drop("injector_model_and_manufacturing_company"). \
        drop("turbo_model_and_manufacturing_company"). \
        drop("crankcase_emission_ctrl_device_model_and_manufacturing_company"). \
        drop("ecu_type_and_manufacturing_company"). \
        drop("doc_model_and_manufacturing_company"). \
        drop("scr_model_and_manufacturing_company"). \
        drop("dpf_model_and_manufacturing_company").\
        drop("engine_manufacturer_name").\
        withColumnRenamed("exhaust_orientation", "engine_exhaust_orientation").\
        withColumnRenamed("vehicle_classification", "vehicle_category")


    insuranceOriginDF = readInsuranceDataDF().\
        withColumn("urea_pump_model", when(col("urea_pump_model") == "-",lit(None)).otherwise(col("urea_pump_model"))). \
        withColumn("urea_pump_supplier", when(col("urea_pump_supplier") == "-", lit(None)).otherwise(col("urea_pump_supplier"))). \
        withColumn("carrier_supplier", when(col("carrier_supplier") == "-", lit(None)).otherwise(col("carrier_supplier"))).\
        withColumn("coating_supplier", when(col("coating_supplier") == "-", lit(None)).otherwise(col("coating_supplier"))).\
        withColumn("internal_length_of_carriage", when(col("internal_length_of_carriage") == "-", lit(None)).otherwise(col("internal_length_of_carriage"))).\
        withColumn("internal_width_of_carriage", when(col("internal_width_of_carriage") == "-", lit(None)).otherwise(col("internal_width_of_carriage"))).\
        withColumn("interior_height_of_carriage", when(col("interior_height_of_carriage") == "-", lit(None)).otherwise(col("interior_height_of_carriage"))).\
        groupBy("engine_model", "vehicle_model").\
        agg(collect_set("urea_pump_model").alias("engine_ureapump_model"),
        collect_set("urea_pump_supplier").alias("engine_ureapump_supplier"),
        collect_set("carrier_supplier").alias("engine_carrier"),
        collect_set("coating_supplier").alias("engine_coating"),
        collect_set("market_segments").alias("vehicle_level1_marketsegment"),
        collect_set("spv_segment").alias("vehicle_spv_segment"),
        collect_set("vehicle_categories").alias("vehicle_model_category"),
        collect_set("name_of_the_manufacturer").alias("vehicle_oem"),
        collect_set("chassis_enterprise").alias("vehicle_chassis_oem"),
        collect_set("brand").alias("vehicle_brand"),
        collect_set("vehicle_name").alias("vehicle_name"),
        collect_set("vehicle_profile_length").alias("vechile_length_mm"),
        collect_set("vehicle_profile_width").alias("vechile_width_mm"),
        collect_set("vehicle_profile_height").alias("vechile_height_mm"),
        collect_set("internal_length_of_carriage").alias("vehicle_box_length_mm"),
        collect_set("internal_width_of_carriage").alias("vehicle_box_width_mm"),
        collect_set("interior_height_of_carriage").alias("vehicle_box_height_mm"),
        collect_set("wheelbase").alias("vehicle_wheelbase_mm"),
        collect_set("drive").alias("vehicle_drive_type"),
        collect_set("number_of_leaf_spring").alias("vehicle_leafspring"),
        collect_set("total_mass").alias("vehicle_gross_weight_kg"),
        collect_set("curb_quality").alias("vehicle_curb_weight_kg"),
        collect_set("quasi-traction_quality").alias("vehicle_quasi-traction_weight_kg"),
        collect_set("number_of_axes").alias("vehicle_axes"),
        collect_set("tire_specifications").alias("vehicle_tire_size"),
        collect_set("number_of_tires").alias("vehicle_tire"),
        collect_set("secondary_market_segment").alias("vehicle_level2_marketsegment"),
        collect_set("three-tier_market_segment").alias("vehicle_level3_marketsegment")).\
        withColumn("engine_ureapump_model", concat_ws(",", "engine_ureapump_model")).\
        withColumn("engine_ureapump_supplier", concat_ws(",", "engine_ureapump_supplier")).\
        withColumn("engine_carrier", concat_ws(",", "engine_carrier")).\
        withColumn("engine_coating", concat_ws(",", "engine_coating")).\
        withColumn("vehicle_box_length_mm", concat_ws(",", "vehicle_box_length_mm")).\
        withColumn("vehicle_box_width_mm", concat_ws(",", "vehicle_box_width_mm")).\
        withColumn("vehicle_box_height_mm", concat_ws(",", "vehicle_box_height_mm")).\
        withColumn("vehicle_leafspring", concat_ws(",", "vehicle_leafspring")).\
        withColumn("vehicle_tire_size", concat_ws(",", "vehicle_tire_size"))



    bulletinJoinWithInsuranceDF = engineBulletinOriginDF.join(insuranceOriginDF,
        (engineBulletinOriginDF.engine_type == insuranceOriginDF.engine_model) &  (engineBulletinOriginDF.vehicle_type == insuranceOriginDF.vehicle_model), "left" )


    buildOriginDF = readRelEngineDetailDF().withColumn("build_date", to_date("build_date")).filter(col("EMISSION_LEVEL").like("%6%")).\
        drop("ENGINE_PLATFORM").drop("EMISSION_LEVEL")
    baseJoinWithBuildDF = buildOriginDF.join(bulletinJoinWithInsuranceDF, buildOriginDF.ESN_NEW == bulletinJoinWithInsuranceDF.esn, "right").\
        drop("ESN_NEW").\
        withColumnRenamed("build_date", "engine_build_date").\
        withColumnRenamed("SO_NUM", "engine_so")

    #efpa_ats table
    efpaAtsOriginDF = readTelEfpaAtsDF()
    efpaBroadCast = broadcast(efpaAtsOriginDF)
    baseJoinWithEfpaDF =  baseJoinWithBuildDF.join(efpaBroadCast, on="esn",  how="left").\
        drop("EFPA_OCCURRENCE_TIME").withColumnRenamed("EFPA_ATS_SERIAL_NUMBER", "engine_atsn")

    #esn_master_detail table
    esnMasterOriginDF = readEsnMasterDetailDF().filter(col("emission_level")=="NSVI").drop("emission_level").\
        withColumnRenamed("rear_axle_ratio", "vehicle_rear_axle_ratio").distinct()
    baseJoinWithEsnMasterDF = baseJoinWithEfpaDF.join(esnMasterOriginDF, on="esn", how= "left")

    #csu_reg_table
    csuRegOriginDF = readCsuRegDF().distinct()
    baseJoinWithCsuRegDF = csuRegOriginDF.join(baseJoinWithEsnMasterDF, on="esn", how= "right").withColumnRenamed("hmiType", "vehicle_hmi_type")

    #engine_base_detail table
    engineBaseDetailOriginDF = readEngineBaseDetailDF().\
        withColumnRenamed("VEHICLE_MODEL" , "vehicle_strain").\
        withColumnRenamed("create_time" ,"vehicle_hb_create_time").distinct()
    engineBaseLower = engineBaseDetailOriginDF.select([col(x).alias(x.lower()) for x in engineBaseDetailOriginDF.columns])  # to lower case of column names
    baseJoinWithEngineBaseDF = engineBaseLower.join(baseJoinWithCsuRegDF, engineBaseLower.engine_serial_num == baseJoinWithCsuRegDF.esn, "right").\
        drop("engine_serial_num").distinct()

    hivePath = "wasbs://aadecdlcnprdspark01@saaa01cnprd19909.blob.core.chinacloudapi.cn/hive/warehouse/de_labeling/labeling_basic_attributes/"
    saveAsTable(baseJoinWithEngineBaseDF, "de_labeling", "labeling_basic_attributes", hivePath)




def readEngineBulletinDF():
    df  = spark.table("de_labeling.engine_bulletin").\
        select("esn", "engine_type", "engine_manufacturer_name", "fuel_injection_pump_model_and_manufacturing_company",
               "common_rail_tube_model_and_manufacturing_company", "injector_model_and_manufacturing_company",
               "turbo_model_and_manufacturing_company", "crankcase_emission_ctrl_device_model_and_manufacturing_company",
               "ecu_type_and_manufacturing_company", "doc_model_and_manufacturing_company","scr_model_and_manufacturing_company",
               "dpf_model_and_manufacturing_company", "exhaust_orientation", "vehicle_type", "vehicle_classification")
    return df


def readRelEngineDetailDF():
    df = spark.table("qa_prd_primary.rel_engine_detail").\
        select("ESN_NEW", "BUILD_DATE", "SO_NUM", "ENGINE_PLATFORM", "EMISSION_LEVEL")
    return df

def readTelEfpaAtsDF():
    return spark.table("qa_prd_reference.ref_efpa_ats_serial_number")


def readInsuranceDataDF():
    df = spark.table("de_labeling.insurance_data_dropduplicate").\
        select("engine_model", "vehicle_model" ,"urea_pump_model", "urea_pump_supplier", "carrier_supplier", "coating_supplier", "name_of_the_manufacturer",
               "market_segments", "spv_segment", "vehicle_categories", "chassis_enterprise", "brand",
               "vehicle_name", "vehicle_profile_length", "vehicle_profile_width", "vehicle_profile_height",
               "internal_length_of_carriage", "internal_width_of_carriage", "interior_height_of_carriage",
               "wheelbase", "drive", "number_of_leaf_spring", "total_mass", "curb_quality", "quasi-traction_quality",
               "number_of_axes", "tire_specifications", "number_of_tires", "secondary_market_segment", "three-tier_market_segment")
    return df


def readEsnMasterDetailDF():
    df = spark.table("pri_tel.esn_master_detail").\
        select("esn", "rear_axle_ratio", "emission_level")
    return df

"""
>>> csu.groupBy("hmiType").count().show()
+--------+------+
| hmiType| count|
+--------+------+
|    none|203188|
|tspBasic|197973|
|       1|    71|
|    NONE|     4|
+--------+------+
>>> csu.filter(csu.ESN == '69808546').show()
+--------+-------+---+----+-----------------+-----------------+-----------+-----+----------+--------+--------------------+----------+
|     ESN|Company|TSP| OEM|              VIN|          EquipID|      BoxID|addrs|     calID| hmiType|CapabilityCreateTime|UpdateDate|
+--------+-------+---+----+-----------------+-----------------+-----------+-----+----------+--------+--------------------+----------+
|69808546|   DCEC|CTY|DFLZ|LNXAEG091KL627351|       1001181112|88002500775|    0|KI10001.02|tspBasic|2019-07-05 09:08:...|  20200911|
|69808546|   DCEC|CTY|DFLZ|LNXAEG091KL627351|LNXAEG091KL627351|88002500775|    0|KI10001.02|    none|2019-07-05 09:08:...|  20200721|
|69808546|   DCEC|CTY|DFLZ|LNXAEG091KL627351|       1001181112|88002500775|    0|KI10001.02|tspBasic|2019-07-05 09:08:...|  20200908|
|69808546|   DCEC|CTY|DFLZ|LNXAEG091KL627351|       1001181112|88002500775|    0|KI10001.02|tspBasic|2019-07-05 09:08:...|  20200907|
|69808546|   DCEC|CTY|DFLZ|LNXAEG091KL627351|LNXAEG091KL627351|88002500775|    0|KI10001.02|tspBasic|2019-07-05 09:08:...|  20200825|
|69808546|   DCEC|CTY|DFLZ|LNXAEG091KL627351|LNXAEG091KL627351|88002500775|    0|KI10001.02|    none|2019-07-05 09:08:...|  20200410|
|69808546|   DCEC|CTY|DFLZ|LNXAEG091KL627351|LNXAEG091KL627351|10012500775|    0|KI10001.02|    none|2019-07-05 09:08:...|  20191230|
+--------+-------+---+----+-----------------+-----------------+-----------+-----+----------+--------+--------------------+----------+


"""
def readCsuRegDF():
    df =  spark.table("pri_tel.tel_csu_reg").\
        select("ESN", "hmiType")
    return df

def readEngineBaseDetailDF():
    df = spark.table("de_labeling.engine_base_detail").\
        select("ENGINE_SERIAL_NUM", "CREATE_TIME", "VEHICLE_MODEL")
    return df



def saveAsTable(df, db, tbl, hivePath):
    df.write.format("parquet") \
        .mode("overwrite") \
        .option('path', hivePath) \
        .saveAsTable(db + "." + tbl)



if __name__ == "__main__":
    main()

