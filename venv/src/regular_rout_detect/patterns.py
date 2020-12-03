# -*- coding: utf-8 -*-
"""
Created on Tue Sep  8 13:21:11 2020

@author: sa187
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import time
import math
from sklearn.cluster import DBSCAN
import matplotlib.animation as animation
np.set_printoptions(precision=6,suppress=True)

def timediff(time1,time2):
    dmo = int(time2[5:7])-int(time1[5:7])
    dda = int(time2[8:10])-int(time1[8:10])
    dho = int(time2[11:13])-int(time1[11:13])
    dmi = int(time2[14:16])-int(time1[14:16])
    dse = int(time2[17:19])-int(time1[17:19])
    if dmo>0:   
        if int(time1[5:7]) in [1,3,5,7,8,10,12]:
            dmo = 31
        elif int(time1[5:7]) in [4,6,9,11]:
            dmo = 30
        else:
            dmo = 28
    return((dmo+dda)*24+dho+dmi/60+dse/3600)
    
#def geodist(coo1,coo2):#km
#    lon1, lat1, lon2, lat2 = map(math.radians, np.hstack([coo1.astype(float),coo2.astype(float)]))
#    dlon=lon2-lon1
#    dlat=lat2-lat1
#    a=math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
#    distance=2*math.asin(math.sqrt(a))*6371*1000
#    distance=round(distance/1000,3)
#    return distance
#
#def eucdist(coo1,coo2):# km
#    lon1, lat1, lon2, lat2 = np.hstack([coo1.astype(float),coo2.astype(float)])
#    return(math.sqrt((lon1-lon2)**2+(lat1-lat2)**2)*100)
#    
#def findneibo(ind,cdata,epsilon):
#    out = []
#    for i in range(len(cdata)):
#        if eucdist(cdata[ind],cdata[i])<epsilon:
#            out.append(i)
#    out.remove(ind)
#    return out

df = pd.read_csv('data_all.csv','\t',encoding = 'utf8',header=None)
df = df.fillna(0)
df = df.sort_values(by = [0,1])
data = np.asarray(df)
data[:,2:5]=data[:,[4,2,3]]
data = data[(data[:,2:5]!=0).min(1)]

esns = np.unique(data[:,0])
n = len(esns)

#for esnind in range(n):
if True:
    esnind = 90
    esn = esns[esnind]
    esndata = data[data[:,0]==esn]
    m = len(esndata)
    
#    fig = plt.figure(figsize = [20,9])
#    plt.plot(esndata[:,3],esndata[:,4])
#    
#    
#    fig = plt.figure(figsize = [20,9])
#    plt.grid('on')
#    plt.xlim([108.6,109.8])
#    plt.ylim([34.2,35.0])
#    for i in range(m):
#        plt.plot(esndata[:i,3],esndata[:i,4],'o-')
#        plt.pause(0.01)
# 
#    
#    def update_points(num):
#        point_ani.set_data(esndata[0:num,3], esndata[0:num,4])
#        return point_ani,
#    	 
#    fig = plt.figure(tight_layout=True,figsize = [20,9])
#    plt.grid('on')
#    plt.xlim([116.576,116.585])
#    plt.ylim([39.924,39.933])
#    point_ani, = plt.plot(esndata[0,3], esndata[0,4], "ro",markersize = 5)
#    plt.grid(ls="--")
#    ani = animation.FuncAnimation(fig, update_points, np.arange(0,m), interval=100, blit=True)
#    plt.show()
#    
#    numpts = 6
#    epsilon = 0.001
#    cdata = esndata[:,3:5]
#    
#    time1 = time.time()
#    nei = []
#    cores = []
#    for i in range(m):
#        nei.append(findneibo(i,cdata,epsilon))
#        print(i)
#        if len(nei[i])>(numpts-1):
#            cores.append(i)
#            
#    t1 = time.time()-time1
#    
#    todo = []
#    flag = -1*np.ones((m,1)).astype(int)
#    clusterid = 0
#    while sum(flag[cores]==-1)>0:
#        temp = cores*(flag[cores]==-1).reshape(1,-1)
#        temp = temp[temp!=0]
#        todo = np.append(todo,temp[0]).astype(int)
#        while len(todo)>0:
#            ind = todo[0]
#            candidates = nei[todo[0]]
#            if ind in cores:
#                todo = np.append(todo,[each for each in candidates if flag[each]==-1]).astype(int)
#            todo = np.unique(todo)
#            print(str(ind)+'\t'+str(clusterid))
#            flag[ind] = clusterid
#            todo = np.delete(todo,np.where(todo==ind))
#        clusterid += 1
#        
#    t2 = time.time()-time1
#            
#    plt.figure(figsize=[13,13])
#    colors = np.array([plt.cm.Spectral(each) for each in np.linspace(0, 1, len(np.unique(flag)))])
#    for i in np.unique(flag):
#        plt.scatter(cdata[np.where(flag==i)[0],0], cdata[np.where(flag==i)[0],1],s = 100,c = colors[i+1].reshape((1,4)))
#    plt.show()
#    print(t1)
#    print(t2)
#    
#    time2 = time.time()
#    db = DBSCAN(min_samples = numpts, eps = epsilon/1000*1000,p = 2).fit(cdata)# epsilon 15:1
#    print(time.time()-time2)
#    flag2 = db.labels_
#    
#    colors = np.array([plt.cm.Spectral(each) for each in np.linspace(0, 1, len(np.unique(db.labels_)))])
#    plt.figure(figsize=[13,13])
#    for i in np.unique(db.labels_):
#        plt.scatter(cdata[np.where(db.labels_==i),0], cdata[np.where(db.labels_==i),1],s = 100,c = colors[i+1].reshape((1,4)))
#    plt.show()
    
    
    # 90
#    time3 = time.time()
    pres = 100
    d1 = np.hstack((esndata[:,1:2],np.round(esndata[:,3:5].astype(float)*pres)/pres,np.arange(len(esndata)).reshape(-1,1))) 
    d1 = d1[np.lexsort(d1[:,[0,1,2]].T)]
    routes = np.unique(d1[:,1:3].astype(float),axis = 0)
    routestype = np.zeros((len(routes),1),dtype = int)
    for i in range(len(routes)):
        d2 = d1[(d1[:,1]==routes[i,0])*(d1[:,2]==routes[i,1])]
        for j in range(1,len(d2)):
            if timediff(d2[j-1,0],d2[j,0])>1:
                routestype[i] +=1

#    plt.figure(figsize = [13,13])
#    plt.scatter(routes[:,0],routes[:,1],c = routestype.flatten(),cmap = 'winter')
#    plt.grid('on')
#    
#    plt.figure(figsize=[13,13])
#    plt.hist(routestype)
#    print(time.time()-time3)
#    print(np.mean(routestype))
