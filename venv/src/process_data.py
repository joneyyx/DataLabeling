import org.apache.spark.sql.functions.{col, lit, when}
import org.apache.spark.sql.types.{DataType, StringType}
import org.apache.spark.sql._


def formatCapitalizeNames(dataFrame: DataFrame): DataFrame = {
val capitalizedNames = dataFrame.columns.map(colname => colname.replaceAll("[(|)]", "").replaceAll(" ", "_").replaceAll("/", "").replaceAll("\\\\", "").replaceAll("-", "_").toUpperCase)
val originalNames = dataFrame.columns
dataFrame.select(List.tabulate(originalNames.length)(i => col(originalNames(i)).as(capitalizedNames(i))): _*)
}


def getWarehousePath(warehouseRootPath: String, database: String = "default", tableName: String): String = {
var dbname = database.toLowerCase
var layer = "na"

warehouseRootPath.toLowerCase + "/"  + dbname + "/" + tableName.toLowerCase
}

def saveDataWithMultipePartitions(df: DataFrame, db: String, tbl: String, path: String, ptkey: List[String] ,mode: String = "overwrite", format: String = "parquet"): Unit = {
formatCapitalizeNames(df).write.
options(Map("path" -> getWarehousePath(path, db, tbl))).
mode(mode).format(format).partitionBy(ptkey(0), ptkey(1)).
saveAsTable(s"$db.$tbl")
}



val hivePath = "wasbs://aadecdlcnprdspark01@saaa01cnprd19909.blob.core.chinacloudapi.cn/hive/warehouse/"
val raw = spark.read.format("csv").option("header", "true").load("wasbs://aadecdlcnprdspark01@saaa01cnprd19909.blob.core.chinacloudapi.cn/hive/warehouse/reference/crab_ESN.csv").withColumnRenamed("VIN", "Crab_VIN")
val engine_base_detail = spark.read.format("csv").option("header", "true").load("wasbs://aadecdlcnprdspark01@saaa01cnprd19909.blob.core.chinacloudapi.cn/hive/warehouse/reference/engine_base_detail.csv")
val esn_base_df = raw.join(engine_base_detail, raw("ESN")===engine_base_detail("ENGINE_SERIAL_NUM"), "left")

val build = spark.table("qa_prd_primary.rel_engine_detail").select("ESN_NEW", "build_date", "ACTUAL_IN_SERVICE_DATE", "IN_SERVICE_DATE_AVE_TIME_LAG", "engine_platform", "engine_plant")
val esn_base_df_2 = esn_base_df.join(build, esn_base_df("ESN")===build("ESN_NEW"), "left")

+++++++++++++++
def saveData(df: DataFrame, db: String, tbl: String, path: String ,mode: String = "overwrite", format: String = "parquet"): Unit = {
formatCapitalizeNames(df).write.
options(Map("path" -> getWarehousePath(path, db, tbl))).
mode(mode).format(format).
saveAsTable(s"$db.$tbl")
}
saveData(esn_base_df_2, "de_labeling", "vehicle_detailed_information", hivePath)
# +++++++++++++++
# val broadcastElem = spark.sparkContext.broadcast(esn_base_df_2)
# val broadcastData = broadcastElem.value
#
# // 20200701- 20200731

var start_date = 20200727
while  (start_date <= 20200731){
var data = spark.sql(s"select * from data_label.tel_hb_labeled where report_date = ${start_date}")
var joinDF = data.join(broadcastData, broadcastData("ESN")===data("Engine_Serial_Number"),"right")
val ptkeys = List("engine_model_group","report_date")
saveDataWithMultipePartitions(joinDF, "data_label", "tel_hb_labeled_Sample",hivePath, ptkeys, "append")
start_date =  start_date +1
}





val prop=new java.util.Properties()
prop.put("user","root")
prop.put("password","root")
val url="jdbc:mysql://10.177.34.34:port/nsvi"

val df=spark.read.jdbc(url,"nsvi_esn_vin",prop)
df.show()
