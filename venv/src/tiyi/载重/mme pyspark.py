# -*- coding: utf-8 -*-
"""
Created on Mon Oct 19 10:34:14 2020

@author: sa187
"""

from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.feature import VectorIndexer
from pyspark.ml.feature import VectorAssembler
import numpy as np
from pyspark.mllib.tree import RandomForest

data = sql('select tel_efpa.espdave, tel_efpa.vspdave, tel_efpa.torqueave, tel_efpa.exhmfave, tel_efpa.exhpressmean, tel_efpa.aatmean, tel_efpa.citmean, tel_efpa.engworknonregentime, tel_efpa.loadfactorave, tel_efpa.chargeflowmean, tel_efpa.engworktime, tel_efpa.idletime, tel_efpa.idlespeedave, tel_efpa.idletorqueave, tel_efpa.idlefuelrateave, tel_efpa.tmfexh, tel_efpa.exhvfmaxnonregen, tel_efpa.fuelvolumetotal, tel_efpa.timescrtmaggresive, tel_efpa.timescrtmmoderate, tel_efpa.timebasechi, tel_efpa.coolanttempave, tel_efpa.coolanttempmax, tel_efpa.coolanttemp90, tel_efpa.chargetmax, tel_efpa.chargetmean, tel_efpa.turbospeedmax, tel_efpa.turbospeedmean, tel_efpa.ambientairpressmean, tel_efpa.aatmin, tel_efpa.aatmax, tel_efpa.railpressmean, tel_efpa.railpressmax, tel_efpa.torquemax, tel_efpa.citmax, tel_efpa.engmotoring, tel_efpa.exhpressmaxnonbraking, tel_efpa.cotmax, tel_efpa.airfuelratemean, tel_efpa.idletimecntmax, tel_efpa.cipmean, tel_efpa.tmfwork, tel_efpa.turbospeedestmean, tel_efpa.chargepressmean, tel_efpa.espdmax, tel_efpa.vspdmax, tel_efpa.hcdosingvolumetotal, tel_efpa.tmffaf, tel_efpa.exhpressmaxnonbrakingoscar, tel_efpa.chargepressmax, tel_efpa.ccpotorquecurve, tel_efpa.exhtempmax, tel_efpa.idleover20minscount, tel_efpa.accpedalgt90over1mincount, tel_efpa.chargepressmin, tel_efpa.timeabovetslimit, tel_efpa.rtcstart, tel_efpa.timeccpo16,tel_efpa.mmemean from pri_tel.tel_efpa').cache()

trainingData, testData= data.randomSplit([0.75, 0.25])

# "'"+"','".join([i[9:] for i in a.split(', ')])+"'"

#inputCols are multiple features
#VectorAssember combines all the inputCols, and return an output Feature which combines all
df_assembler = VectorAssembler(inputCols=['espdave','vspdave','torqueave','exhmfave','exhpressmean','aatmean','citmean','engworknonregentime','loadfactorave','chargeflowmean','engworktime','idletime','idlespeedave','idletorqueave','idlefuelrateave','tmfexh','exhvfmaxnonregen','fuelvolumetotal','timescrtmaggresive','timescrtmmoderate','timebasechi','coolanttempave','coolanttempmax','coolanttemp90','chargetmax','chargetmean','turbospeedmax','turbospeedmean','ambientairpressmean','aatmin','aatmax','railpressmean','railpressmax','torquemax','citmax','engmotoring','exhpressmaxnonbraking','cotmax','airfuelratemean','idletimecntmax','cipmean','tmfwork','turbospeedestmean','chargepressmean','espdmax','vspdmax','hcdosingvolumetotal','tmffaf','exhpressmaxnonbrakingoscar','chargepressmax','ccpotorquecurve','exhtempmax','idleover20minscount','accpedalgt90over1mincount','chargepressmin','timeabovetslimit','rtcstart','timeccpo16'], outputCol="features")

# data = data.withColumn("NULL_COUNT", np.sum(data[c].isNull().cast('int') for c in data.columns))
# data = data.filter('NULL_COUNT==0')
# data = data.drop('NULL_COUNT')
# data = df_assembler.transform(data)
#
# data = data.select(['tel_efpa.mmemean','features'])
# data = data.withColumnRenamed('mmemean','label')

addNullCountDF = trainingData.withColumn("null_count", np.sum(trainingData[c].isNull().cast('int') for c in trainingData.columns))
inputDF = addNullCountDF.filter(addNullCountDF.null_count == '0').drop("null_count")
after_assembler_df = df_assembler.transform(inputDF)

trainDF = after_assembler_df.select("mmemean", "features").withColumnRenamed('mmemean','label')

# model = RandomForest.trainRegressor(after_assembler_df, categoricalFeaturesInfo={},
#                                     numTrees=3, featureSubsetStrategy="auto",
#                                     impurity='variance', maxDepth=4, maxBins=32)



rf = RandomForestRegressor(featuresCol="features")

rf.fit(data)
rf.write().save("/jiarui/rf")

