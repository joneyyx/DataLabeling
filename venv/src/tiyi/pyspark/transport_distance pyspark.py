from pyspark.sql.types import LongType
from pyspark.sql.types import IntegerType, StringType, FloatType

from pyspark.sql import functions as F
from pyspark.sql.window import Window
import math
import time
"""
运距pyspark
TODO，运距里面的几何中心改成车窝
"""
t0 = time.time()
def geodistance(lon1,lat1,lon2,lat2):
    lon1, lat1, lon2, lat2 = map(math.radians, [float(lon1), float(lat1), float(lon2), float(lat2)])
    dlon=lon2-lon1
    dlat=lat2-lat1
    a=math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
    distance=2*math.asin(math.sqrt(a))*6371*1000  
    distance=round(distance/1000,3)
    return distance

geod = F.udf(geodistance, FloatType())

def m_partition(dis):
    if dis<200:
        return('city')
    elif dis <400:
        return('branch')
    elif dis>400:
        return('trunk')

m_partition = F.udf(m_partition,StringType())

#,76151279(76152726)
data = sql('select engine_serial_number,occurrence_date_time,longitude,latitude,high_resolution_total_vehicle_distance from de_labeling.tel_hb_labeled_sample where report_date>20200630 and report_date<20200801').cache()

data = data.filter((data['longitude']!=0) & (data['latitude']!=0))

g2r = math.pi/180
data = data.withColumn('x',F.cos(data['latitude']*g2r)*F.cos(data['longitude']*g2r))
data = data.withColumn('y',F.cos(data['latitude']*g2r)*F.sin(data['longitude']*g2r))
data = data.withColumn('z',F.sin(data['latitude']*g2r))


"c里面是几何中心，TODO需要改成patterns里面算出来的车窝"
c = data.groupBy('engine_serial_number').agg(F.mean('x').alias('x'),F.mean('y').alias('y'),F.mean('z').alias('z'))
c = c.withColumn('clon',F.atan(c['y']/c['x'])/g2r+180)
c = c.withColumn('clat',F.atan(c['z']/F.sqrt(c['x']*c['x']+c['y']*c['y']))/g2r)
c = c.drop('x','y','z')



#TODO 直接用车窝替代
#将里程规整到100米
data = data.drop('x','y','z')
data = data.withColumn('dis',(data["high_resolution_total_vehicle_distance"]/100).cast(IntegerType())*100)
data = data.withColumn('time',F.unix_timestamp('occurrence_date_time').cast(IntegerType()))

temp = data.groupBy('dis','engine_serial_number').agg((F.max(data['time'])-F.min(data['time'])).alias('staydur'))

data = data.withColumn('rownumber',F.row_number().over(Window.partitionBy('engine_serial_number','dis').orderBy(data['time'].asc())))
data = data.join(temp,['engine_serial_number','dis'],'left')


sites = data.filter((data['rownumber']==1) & (data['staydur']>1800)).cache()
sites = sites.drop('rownumber','dis')
data = data.drop('rownumber','dis')

sites = sites.join(c,'engine_serial_number','left')
sites = sites.withColumn('d1',geod('longitude','latitude','clon','clat'))
d1 = sites.groupBy('engine_serial_number').agg(F.max('d1').alias('d1result'))

sites = sites.withColumn('diff',(sites['high_resolution_total_vehicle_distance']-F.lag(sites['high_resolution_total_vehicle_distance'],1).over(Window.orderBy('time').partitionBy('engine_serial_number'))))

d2 = sites.join(d1['engine_serial_number','d1result'],'engine_serial_number','left')
d2 = d2.filter(d2['diff']>100*d2['d1result'])
d2 = d2.groupBy('engine_serial_number').agg(F.mean(d2['diff']/1000).alias('d2result'),(F.stddev(d2['diff'])/1000).alias('std'))


results = d1.join(d2,'engine_serial_number').join(c,'engine_serial_number').withColumn('market_type',m_partition(d1['d1result']))

results.show()


print(time.time()-t0)
data.unpersist()

