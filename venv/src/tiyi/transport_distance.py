# -*- coding: utf-8 -*-
"""
Created on Thu Sep  3 13:33:06 2020
@author: sa187
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import math
import os
import time

def timediff(time1,time2):
    t1 = time.mktime(time.strptime(time1,"%Y-%m-%d %H:%M:%S"))
    t2 = time.mktime(time.strptime(time2,"%Y-%m-%d %H:%M:%S"))
    return(abs(t1-t2)/3600)
    
def center_geolocation(geolocations):
    x = 0
    y = 0
    z = 0
    lenth = len(geolocations)
    for lon, lat in geolocations:
        lon = math.radians(float(lon))
        lat = math.radians(float(lat))
        x += math.cos(lat) * math.cos(lon)
        y += math.cos(lat) * math.sin(lon)
        z += math.sin(lat)
    x = float(x / lenth)
    y = float(y / lenth)
    z = float(z / lenth)
    return (math.degrees(math.atan2(y, x)), math.degrees(math.atan2(z, math.sqrt(x * x + y * y))))

def geodistance(lon1,lat1,lon2,lat2):
    lon1, lat1, lon2, lat2 = map(math.radians, [float(lon1), float(lat1), float(lon2), float(lat2)])
    dlon=lon2-lon1
    dlat=lat2-lat1
    a=math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
    distance=2*math.asin(math.sqrt(a))*6371*1000  #地球平均半径，6371km
    distance=round(distance/1000,3)
    return distance

def getMaxestDistance(geolocations,center):
    distances=[]
    for lon, lat in geolocations:
        d=geodistance(lon,lat,center[0],center[1])
        distances.append(d)
    # print(distances)
    return max(distances)

def getOnePolyygen(geolocations):
    center=center_geolocation(geolocations)
    neartDistance=getMaxestDistance(geolocations,center)
    return center,neartDistance

#data = np.asarray(pd.read_csv('data_all.csv','\t',encoding = 'utf8',header=None))
#data[:,2:5]=data[:,[4,2,3]]
#data = data[~np.isnan(data[:,2:5].astype(float)).max(1)]
#data[:,3:5] = np.round(data[:,3:5].astype(float),decimals = 3)
#data = data[np.lexsort(data[:,[1,0]].T)]
#data = data[(data[:,2:5]!=0).min(1)]
    # anomaly 8 31 38 50 6 
    # repeated 11 12 
    # normal 36 37 
    # adverse 41 171
    # real journey 45
    # segmented 48 116 126 147 170 140 76 117
    # mixed 178 57 48 93 115 59 180 5 100 72


os.chdir(r'C:\Users\SA187\Documents\My Received Files\8.31zm算法') 
np.set_printoptions(precision=6,suppress=True,threshold=np.inf)

# select engine_serial_number, occurrence_date_time, longitude, latitude, high_resolution_total_vehicle_distance from data_label.tel_hb_labeled_sample where engine_serial_number<76160000 and report_date>20200630 and report_date<20200801
df = pd.read_csv('data_new.csv','\\t',encoding = 'utf8',header=0)
data = np.asarray(df)
data[:,2:5]=data[:,[4,2,3]]
data[:,3:5] = np.round(data[:,3:5].astype(float),decimals = 4)
data = data[np.lexsort(data[:,[1,0]].T)]
data = data[~np.isnan(data[:,2:5].astype(float)).max(1)]
data = data[(data[:,2:5]!=0).min(1)]


esns = np.unique(data[:,0])
n = len(esns)
d1 = np.zeros((n,1))
d2 = np.zeros((n,1))
stds = np.zeros((n,1))
centers = np.zeros((n,2))
for esnind in range(n):
    esn = esns[esnind]
    esndata = data[data[:,0]==esn]
    
    centers[esnind,:],d1[esnind] = getOnePolyygen(esndata[:,3:5])
    sites = np.zeros(shape = (0,6))
    miles = np.unique((esndata[:,2]/100).astype(np.int)*100)
    for i in miles:
        d = esndata[:,2]-i
        temp = esndata[(d>=0)*(d<100),:]
        if temp.size:
            td = timediff(temp[0,1],temp[-1,1])
            if td>0.5:
                sites = np.append(sites,[list(np.append(temp[0],td))],axis = 0)
    
    dists = sites[1:,2].astype(np.float)-sites[0:-1,2].astype(np.float)
    dists = np.delete(dists,np.where(dists<d1[esnind]*100))
    
    d2[esnind] = np.nan_to_num(np.mean(dists)/1000)
    stds[esnind] = np.nan_to_num(np.std(dists)/1000)
    print(esnind)
    
print(d1[esnind])
print(d2[esnind])
print(stds[esnind])
print(centers[esnind])


plt.figure()
plt.get_current_fig_manager().window.showMaximized()
plt.hist(dists,bins = 40)
plt.title(esn)

plt.figure()
plt.get_current_fig_manager().window.showMaximized()
plt.plot(esndata[:,3],esndata[:,4],'b')
plt.plot(sites[:,3].astype(np.float),sites[:,4].astype(np.float),'o-r')
plt.plot(centers[esnind,0],centers[esnind,1],'*y',markersize = 25)
plt.title(esn)
plt.axis('equal')
plt.grid('on')
plt.xlabel('d1 = '+d1[esnind].astype(str)+'\n'+'d2 = '+d2[esnind].astype(str))


diff = abs(d1.astype(np.float)-d2.astype(np.float))
diffind = np.argsort(diff,axis = 0)
results = np.hstack((esns[:,None],d1,d2,diff,diffind,stds,centers))
#pd.DataFrame(results).to_csv('transport_distance results_new.csv',header = None,index = False)

results = pd.read_csv('transport_distance results_new.csv',',',encoding = 'utf8',header = None)
d1 = np.asarray(results.iloc[:,0])
d2 = np.asarray(results.iloc[:,1])
diff = np.asarray(results.iloc[:,2])
diffind = np.asarray(results.iloc[:,3])
stds = np.asarray(results.iloc[:,4])
centers = np.asarray(results.iloc[:,5:7])

plt.figure()
plt.get_current_fig_manager().window.showMaximized()
plt.plot(range(n),d1[diffind,:].flatten())
plt.plot(range(n),d2[diffind,:].flatten())
plt.legend(['1','2'])
