import org.apache.spark.sql.functions.

{col, lit, when}
import org.apache.spark.sql.types.

{DataType, StringType}
import org.apache.spark.sql._


def formatCapitalizeNames(dataFrame: DataFrame): DataFrame = {
    val


capitalizedNames = dataFrame.columns.map(colname= > colname.replaceAll("[(|)]", "").replaceAll(" ", "_").replaceAll("/",
                                                                                                                    "").replaceAll(
    "\\\\", "").replaceAll("-", "_").toUpperCase)
val
originalNames = dataFrame.columns
dataFrame.select(List.tabulate(originalNames.length)(i= > col(originalNames(i)).as(capitalizedNames(i))): _ *)
}

def getWarehousePath(warehouseRootPath: String, database: String = "default", tableName: String): String = {
    var


dbname = database.toLowerCase
var
layer = "na"

warehouseRootPath.toLowerCase + "/" + dbname + "/" + tableName.toLowerCase
}

def saveData(df: DataFrame, db: String, tbl: String, path: String, mode: String = "overwrite",
             format: String = "parquet"): Unit = {
    formatCapitalizeNames(df).write.
        options(Map("path"

-> getWarehousePath(path, db, tbl))).
mode(mode).format(format).
    saveAsTable(s
"$db.$tbl")
}

// import java.sql.Timestamp
// case


class Name(engine_serial_number: String, occurrence_date_time: Timestamp

, model: String, displacement: String, Engine_Model: String, engine_speed: String, high_resolution_total_vehicle_distance: String, report_date: String, engine_name: String, engine_model_group: String)
// var
blankDF = Seq.empty[Name].toDF()

val
esnlist = List(76536773,
               76538778,
               76544851,
               76548135,
               76548335,
               76551419,
               76552836,
               76560894,
               76561792,
               76561796,
               76564298,
               76566243,
               76614765,
               76614786,
               76617330,
               76617420,
               76617438,
               76617672,
               76617957,
               76618634,
               76625264,
               76625878,
               76625926,
               76625948,
               76627450,
               76628097,
               76630667,
               76630721,
               76631113,
               76633952,
               76634893,
               76637980,
               76740242,
               76740320,
               76742497,
               76742828,
               76745391,
               76746332,
               76746498,
               82009931,
               82010621,
               82017187,
               82018816,
               82020281,
               82023571,
               82024251,
               82025300,
               82025632,
               82026278,
               82026894,
               82027560,
               82027583,
               82029077,
               82032536,
               82033304,
               82033751,
               82034260,
               82034266,
               82034600,
               82036859,
               82037762,
               82043800,
               82050850,
               82055394,
               76535922,
               76537367,
               76539227,
               76542598,
               76548249,
               76548333,
               76551787,
               76554845,
               76561646,
               76564787,
               76614365,
               76614789,
               76614796,
               76614842,
               76615387,
               76615512,
               76615553,
               76615580,
               76616355,
               76617010,
               76617932,
               76617985,
               76618938,
               76619986,
               76621409,
               76626237,
               76626254,
               76627030,
               76627663,
               76627727,
               76629608,
               76629680,
               76629683,
               76630458,
               76631406,
               76631443)

val
build = spark.table("qa_prd_primary.rel_engine_detail").select("ESN_NEW", "build_date", "ACTUAL_IN_SERVICE_DATE",
                                                               "IN_SERVICE_DATE_AVE_TIME_LAG", "engine_platform",
                                                               "engine_plant")
val
build_sample = build.filter($"ESN_NEW".isin(esnlist: _ *))

val
hivePath = "wasbs://aadecdlcnprdspark01@saaa01cnprd19909.blob.core.chinacloudapi.cn/hive/warehouse/"

// val
hb = spark.sql(s
"select engine_serial_number,occurrence_date_time, model, displacement, Engine_Model ,engine_speed, high_resolution_total_vehicle_distance, report_date, engine_name, engine_model_group from pri_tel.tel_hb_labeled  where  report_date between ${startList(i)} and ${endList(i)}")
// val
startList = List("20200201", "20200301", "20200401", "20200501", "20200601", "20200701", "20200801", "20200901")
// val
endList = List("20200229", "20200331", "20200430", "20200531", "20200630", "20200731", "20200831", "20200930")

for (i < - 20200103 to 20200131){
    println(s"report_date: ${i}")
println(s">>>>>>>>>>>>select engine_serial_number,occurrence_date_time, model, displacement, Engine_Model ,engine_speed, high_resolution_total_vehicle_distance, report_date, engine_name, engine_model_group from pri_tel.tel_hb_labeled  where  report_date ='${i}'")
var hb = spark.sql(s"select engine_serial_number,occurrence_date_time, model, displacement, Engine_Model ,engine_speed, high_resolution_total_vehicle_distance, province, city, latitude, longitude ,report_date, engine_name, engine_model_group from pri_tel.tel_hb_labeled  where  report_date = '${i}'")
var joinDF = build_sample.join(hb, hb("engine_serial_number") == =build_sample("esn_new"))
saveData(joinDF, "de_labeling", "vehicle_purchase_info", hivePath)
println(s"saved: ${i}")
}

