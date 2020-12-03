from pyspark.sql.types import LongType
from pyspark.sql.types import IntegerType
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import times
from pyspark import *

spark = SparkSession.builder.appName("data_Label").enableHiveSupport().getOrCreate()
spark.conf.set("spark.sql.shuffle.partitions", 400)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
spark.conf.set("spark.sql.broadcastTimeout", 36000)
spark.conf.set("spark.rpc.askTimeout", 300)
spark.conf.set("spark.dynamicAllocation.enabled", "true")


t0 = time.time()
data = sql('select engine_serial_number,occurrence_date_time,longitude,latitude from pri_tel.tel_hb_labeled where report_date>20200630 and report_date<20200901').cache()
#》》》》》》》》》》。过滤掉engine_speed =0
data = data.withColumn('lon',(100*data["longitude"]).cast(IntegerType())/100)
data = data.withColumn('lat',(100*data["latitude"]).cast(IntegerType())/100)

data = data.withColumn('time',F.unix_timestamp('occurrence_date_time').cast(IntegerType()))

data = data.withColumn('diff',data['time']-F.lag(data['time'],1,0).over(Window.orderBy('engine_serial_number','time').partitionBy('engine_serial_number','lon','lat')))
#count最大的为车窝
#超过100s
data = data.withColumn('count',F.when(data.diff>100,1))

data = data.drop('longitude','latitude','occurrence_date_time','diff')

b = data.groupBy('engine_serial_number','lon','lat').sum('count')
b = b.na.fill(0)


#线路重复指数->平均值
#线路稳定程度->方差
b = b.groupBy('engine_serial_number').agg(F.mean('sum(count)').alias('mean'),F.stddev('sum(count)').alias('std'))
b.show()


print(time.time()-t0)
data.unpersist()

spark.close()