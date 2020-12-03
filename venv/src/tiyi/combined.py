# -*- coding: utf-8 -*-
"""
Created on Wed Nov  4 17:41:24 2020

@author: SA187
"""

# -*- coding: utf-8 -*-
"""
Created on Tue Sep  8 13:21:11 2020
@author: sa187, 重复指数， 运距+重复指数
"""
# In[] header
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import math
import os
import time
    
def geodist(coo1,coo2):#km
    lon1, lat1, lon2, lat2 = map(math.radians, np.hstack([coo1.astype(float),coo2.astype(float)]))
    dlon=lon2-lon1
    dlat=lat2-lat1
    a=math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
    distance=2*math.asin(math.sqrt(a))*6371*1000
    distance=round(distance/1000,3)
    return distance

def eucdist(coo1,coo2):# km
    lon1, lat1, lon2, lat2 = np.hstack([coo1.astype(float),coo2.astype(float)])
    return(math.sqrt((lon1-lon2)**2+(lat1-lat2)**2)*100)
    
def findneibo(ind,cdata,epsilon):
    out = []
    for i in range(len(cdata)):
        if eucdist(cdata[ind],cdata[i])<epsilon:
            out.append(i)
    out.remove(ind)
    return out

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

def getOnePolyygen(geolocations,center):
    neartDistance=getMaxestDistance(geolocations,center)
    return center,neartDistance


os.chdir(r'C:\Users\SA187\Documents\My Received Files\8.31zm算法') 
np.set_printoptions(precision=6,suppress=True)

# In[] data
''' old data
df = pd.read_csv('data_all.csv','\t',encoding = 'utf8',header=None)
df = df.fillna(0)
df = df.sort_values(by = [0,1])
data = np.asarray(df)
data[:,2:5]=data[:,[4,2,3]]
data = data[(data[:,2:5]!=0).min(1)]
'''
# data processing
df = pd.read_csv('data_new.csv','\t',encoding = 'utf8',header=0)
data = np.asarray(df)
data[:,2:5]=data[:,[4,2,3]]
data[:,3:5] = np.round(data[:,3:5].astype(float),decimals = 4)
data = data[np.lexsort(data[:,[1,0]].T)]
data = data[~np.isnan(data[:,2:5].astype(float)).max(1)]
data = data[(data[:,2:5]!=0).min(1)]

esns = np.unique(data[:,0])
n = len(esns)

# In[] algorithms
plt.close('all')
esns = np.unique(data[:,0])
n = len(esns)
d1 = np.zeros((n,1))
d2 = np.zeros((n,1))
stds = np.zeros((n,1))
centers = np.zeros((n,2))
esnind = 1
for esnind in range(esnind,esnind+1):#range(n)
     esn = esns[esnind]
     esndata = data[data[:,0]==esn]
     n1 = len(esndata)
    
#    plt.figure()
#    plt.get_current_fig_manager().window.showMaximized()
#    plt.plot(esndata[:,3],esndata[:,4])


     pres = 100
     d11 = np.hstack((esndata[:,1:2],np.round(esndata[:,3:5].astype(float)*pres)/pres,np.arange(len(esndata)).reshape(-1,1))) 
     d11 = d11[np.lexsort(d11[:,[0,1,2]].T)]
     routes = np.unique(d11[:,1:3].astype(float),axis = 0)
     routestype = np.zeros((len(routes),1),dtype = int)
     for i in range(len(routes)):
        d12 = d11[(d11[:,1]==routes[i,0])*(d11[:,2]==routes[i,1])]
        for j in range(1,len(d12)):
            if timediff(d12[j-1,0],d12[j,0])>1:
                routestype[i] +=1

     # 车窝
     center = routes[np.where(routestype == max(routestype))[0],][0,]
     
     plt.figure()
     plt.get_current_fig_manager().window.showMaximized()
     plt.scatter(routes[:,0],routes[:,1],c = routestype.flatten(),cmap = 'winter',s = 20)
     plt.scatter(center[0],center[1],c = 'r',marker = '*',cmap = 'winter',s = 500,alpha = 0.7)
     plt.grid('on')
     plt.colorbar()
     plt.xlabel('latitude')
     plt.ylabel('longitude')
     plt.axis('equal')
     plt.title(esns[esnind])
         
     plt.figure()
     plt.get_current_fig_manager().window.showMaximized()
     plt.hist(routestype)
     plt.xlabel('repetition')
     plt.ylabel('# of grids')
     plt.title(esns[esnind])
     print(np.mean(routestype))
     print(center)
    
    
     centers[esnind,:],d1[esnind] = getOnePolyygen(esndata[:,3:5],center)
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
     plt.xlabel('d1 = '+d1[esnind,0].astype(str)+'\n'+'d2 = '+d2[esnind,0].astype(str))