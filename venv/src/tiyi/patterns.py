# -*- coding: utf-8 -*-
# """
# Created on Tue Sep  8 13:21:11 2020
# @author: sa187， 运距
# """
# In[] header
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import math
import os
from sklearn.cluster import DBSCAN
import matplotlib.animation as animation



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
#for esnind in range(n):
    esn = esns[esnind]
    esndata = data[data[:,0]==esn]
    n1 = len(esndata)
    
   plt.figure()
   plt.get_current_fig_manager().window.showMaximized()
   plt.plot(esndata[:,3],esndata[:,4])

   def update_points(num):
       point_ani.set_data(esndata[0:num,3], esndata[0:num,4])
       return point_ani,
   fig = plt.figure(tight_layout = True)
   plt.get_current_fig_manager().window.showMaximized()
   plt.grid(ls="--")
   plt.xlim([min(esndata[:,3]),max(esndata[:,3])])
   plt.ylim([min(esndata[:,4]),max(esndata[:,4])])
   point_ani, = plt.plot(esndata[0,3], esndata[0,4], "ro",markersize = 5)
   ani = animation.FuncAnimation(fig, update_points, np.arange(0,n1), interval=1000, blit=True)
   fig.show()
#    
#    numpts = 6
#    epsilon = 0.001
#    cdata = esndata[:,3:5]
#    
#    nei = []
#    cores = []
#    for i in range(n1):
#        nei.append(findneibo(i,cdata,epsilon))
#        print(i)
#        if len(nei[i])>(numpts-1):
#            cores.append(i)
#    
#    todo = []
#    flag = -1*np.ones((n1,1)).astype(int)
#    clusterid = 0
#    while sum(flag[cores]==-1)>0:
#        temp = cores*(flag[cores]==-1)[None,:]
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
#    plt.figure()
#    plt.get_current_fig_manager().window.showMaximized()
#    colors = np.array([plt.cm.Spectral(each) for each in np.linspace(0, 1, len(np.unique(flag)))])
#    for i in np.unique(flag):
#        plt.scatter(cdata[np.where(flag==i)[0],0], cdata[np.where(flag==i)[0],1],s = 100,c = colors[i+1].reshape((1,4)))
#    plt.show()
#    
#    db = DBSCAN(min_samples = numpts, eps = epsilon/1000*1000,p = 5).fit(cdata)# epsilon 15:1
#
#    flag2 = db.labels_
#    
#    colors = np.array([plt.cm.Spectral(each) for each in np.linspace(0, 1, len(np.unique(db.labels_)))])
#    plt.figure()
#    plt.get_current_fig_manager().window.showMaximized()
#    for i in np.unique(db.labels_):
#        plt.scatter(cdata[np.where(db.labels_==i),0], cdata[np.where(db.labels_==i),1],s = 100,c = colors[i+1].reshape((1,4)))
#    plt.show()
    

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

#    plt.figure()
#    plt.get_current_fig_manager().window.showMaximized()
#    plt.scatter(routes[:,0],routes[:,1],c = routestype.flatten(),cmap = 'winter',s = 20)
#    plt.scatter(routes[np.where(routestype == max(routestype))[0],0],routes[np.where(routestype == max(routestype))[0],1],c = 'r',marker = '*',cmap = 'winter',s = 500,alpha = 0.7)
#    plt.grid('on')
#    plt.colorbar()
#    plt.xlabel('latitude')
#    plt.ylabel('longitude')
#    plt.axis('equal')
#    plt.title(esns[esnind])
    
#    plt.figure()
#    plt.get_current_fig_manager().window.showMaximized()
#    plt.hist(routestype)
#    plt.xlabel('repetition')
#    plt.ylabel('# of grids')
#    plt.title(esns[esnind])
    
    print(np.mean(routestype))
    print(routes[np.where(routestype == max(routestype))[0],])