# -*- coding: utf-8 -*-
"""
Created on Tue Sep 15 10:41:12 2020
@author: SA187
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt,seaborn
from scipy.stats import t
import os
import math
import time
import pickle
from sklearn.ensemble import RandomForestRegressor

os.chdir(r'C:\Users\SA187\Documents\My Received Files\9.14载重') 
np.set_printoptions(precision=6,suppress=True,threshold = 1000)#np.inf)
pd.set_option('display.max_columns', 100,'display.max_rows', 100)


def lm(x, y, xnames = [], PCA = False, nPCA = 0, scaling = False, intercept = False):
    nx,px = x.shape
    out = []
    if PCA:
        if nPCA == 0:
            nPCA = px
        x = x-np.mean(x,axis = 0)
        a,b = np.linalg.eig(np.dot(x.T,x))
        temp = np.argsort(-a)
        a = a[temp]
        b = b[:,temp]
        contri = a.cumsum()/sum(a)
        x = np.dot(x,b[:,0:nPCA])
        xnames = pd.Index(['Component '+str(i+1) for i in range(0,nPCA)])
        out = [contri]
    if scaling:
        x = (x-np.min(x,axis = 0))/(np.max(x,axis = 0)-np.min(x,axis = 0))
    if intercept:
        x = np.hstack([x,np.ones((nx,1))])
    beta = np.dot(np.dot(np.linalg.inv(np.dot(x.T,x)),x.T),y)
    res = y-np.dot(x,beta)
    SST = ((y-np.mean(y))**2).sum()
    SSE = (res**2).sum()
    R2 = 1-SSE/SST/(nx-px)*(nx)
    sigma2 = SSE/(nx-px)
    tval = abs(beta)/np.sqrt(sigma2*np.linalg.inv(np.dot(x.T,x)).diagonal())[:,None]
    tp = 2*(1-t.cdf(tval,nx-px))
    AIC = nx*math.log(SSE/nx)+2*px
    anova = pd.DataFrame(np.hstack([beta,tval,tp]),columns = ['beta','tval','tp'])
    if len(xnames)!=0:
        anova.index = xnames
        anova = anova.sort_values('tval',ascending = False)
    out.extend([beta,res,SST,SSE,R2,sigma2,tval,tp,AIC,anova])
    return(out)
    
def rmse(p,y):
    return(math.sqrt(sum((p-y)**2)/len(y)))
    
#df = pd.read_csv('efpa_data8.csv','\t',encoding = 'utf8')
# 40093 esns
#para = pd.read_excel('MME_EStimation2.xlsx',encoding = 'utf8')
#paras = para['Trend Parameter Name']
#esns = np.unique(df.iloc[:,0])
#
#esninfo = pd.read_csv('esninfo_con.csv',encoding = 'utf8')
##esninfo = esninfo.iloc[np.where(esninfo.iloc[:,0].isin(esns))[0],]
##esns = np.array(esninfo['esn'])
#nesn = len(esns)
#df = df.iloc[np.where(df.iloc[:,0].isin(esns))[0]]
#
#data = df[['tel_efpa.esn']].rename(columns = {'tel_efpa.esn':'esn'})
#
#data = df[['tel_efpa.esn']+['tel_efpa.'+i.lower().strip() for i in paras]]
#data.columns = pd.Index([i[9:] for i in data.columns])
#
##unvalid = np.array([])
##for i in range(len(paras)):
##    name = paras[i].lower().strip()
##    name1 = 'tel_efpa.'+name
##    if name1 in df.columns:
##        data = pd.concat([data,df[[name1]].rename(columns = {name1:name})],axis = 1)
##    else:
##        unvalid = np.append(unvalid,i)
##unvalid = paras[unvalid]
#
#data = pd.merge(data,esninfo[['esn','engine_type']],how = 'inner',on = ['esn','esn'])
#n = len(data)
#data.to_csv('efpa_con.csv',index = False,header = True)
##unvalid.to_csv('unvalid.csv',index = False,header = True)


data = pd.read_csv('efpa_con.csv',',',header = 0,encoding = 'utf8')
esns = np.array(np.unique(data['esn']))
nesn = len(esns)
n = len(data)

data['fuelconsumpper100'] = 100*data['fuelvolumetotal']/data['drivetime']/data['vspdave']*3600
data['totaldist'] = data['drivetime']/3600 * data['vspdave']
data['hcdosingvolumetotal100'] = data['hcdosingvolumetotal']/data['totaldist']
data['ptofuel100'] = data['ptofuelrateave'] * data['ptotime']/data['totaldist']
data = data[~data['engine_type'].isin(['4J28TC6','MC07.27-60','ISZ56051','YCS04200-68','YCS04200-68',np.nan])]
data['power'] = data['engine_type'].str.extract(r'([0-9]{3})\w?$')
data['displacement'] = data['engine_type'].str.extract(r'^\w([0-9]\.?[0-9]?)\w*')
data[['power','displacement']] = data[['power','displacement']].astype('float')
data['etype'] = data['engine_type']

droplist = [i.lower() for i in ['FuelVolumeTotal','PTOFuelRateAve','PTOTime','AmbientAirPressMin','AmbientAirPressmax','HCDosingVolumeTotal','engine_type','mmemax','mmemin']]
data.drop(columns = droplist,inplace = True)

np.isnan(data.drop(columns = ['etype'])).sum(0)/n
nalist = [i.strip().lower() for i in 'EngWorkNonRegenTime,     EngWorkTime,              CCPO24EngBraking,         TimeSCRTMAggresive,       TimeSCRTMModerate,        TimeAlpha0,               TimeBaseChi,                                CCPOTorqueCurve,Idleover20minsCount,AccPedalGT90over1minCount,TimeaboveTSlimit,TimeCCPO16,CountCCPO16GT10sec,ptofuel100'.split(',')]
data[nalist] = data[nalist].fillna(0)
np.isnan(data.drop(columns = ['etype'])).sum(0)/n

data = data.reset_index(drop = True)

# add displacement, scatter yhat and y, evaluate error distribtion, add variable.

yind = np.where(~np.isnan(data['mmemean']) * data['drivetime']>600)[0]
data1 = data.iloc[yind,].drop(columns = ['etype','displacement'])
n1,p1 = data1.shape

np.isnan(data1).sum(0)/n1

x = data1.drop(columns = ['esn','mmemean'])
x = x.iloc[:,np.where(np.isnan(x).sum(0)<n1*0.5)[0]]
xnames = x.columns#.append(pd.Index(['intercept']))
x = x.values
y = data1['mmemean'].values
xind = np.where(np.isnan(x).sum(1)==0)[0]
y = y[xind]
x = x[xind,]
nx,px = x.shape

tind = np.random.choice(range(nx),int(nx*.7),replace=False)
tindc = np.array(list(set(range(nx))-set(tind)))

xtrain = x[tind,];ytrain = y[tind]
xtest = x[tindc,];ytest = y[tindc]

# In[] RF
parval = 'displacement'
etype = data[[parval]].iloc[yind,:].reset_index(drop = True).iloc[xind,:].reset_index(drop = True)
etypetrain = etype.iloc[tind,:].reset_index(drop = True)
etypetest  = etype.iloc[tindc,:].reset_index(drop = True)

etypetrains = etypetrain.drop_duplicates().sort_values([parval])

#inpinfo = etypetrains['etype'].str.extract(r'^\w([0-9]\.?[0-9]?)\w*')
#inpinfo.index = resultse.index
#pd.concat((resultse,inpinfo),axis = 1).groupby([0]).mean()['mses']

train = pd.concat([pd.DataFrame(xtrain,columns = xnames),etypetrain,pd.DataFrame(ytrain,columns = ['mme'])],axis = 1)

#forest = RandomForestRegressor(n_estimators=100, random_state=0, n_jobs=-1)
#RF = forest.fit(xtrain,np.ravel(ytrain))
#
#RFimpor = pd.concat((pd.DataFrame(xnames),pd.DataFrame(RF.feature_importances_,columns = ['importance'])),axis = 1).sort_values(by = ['importance'],ascending = False).reset_index(drop = True)
#RFimpor['cumsum'] = np.cumsum(RFimpor['importance'])

t1 = time.time()
mods = []
yhats = []
mses = []
plots = []
rplots = []
for i in etypetrains.values:
    temp = np.where(etypetrain==i)[0]
    forest = RandomForestRegressor(n_estimators=10, random_state=0, n_jobs=-1)
    mod = forest.fit(xtrain[temp,],ytrain[temp])
    mods.append(mod)
    temp2 = np.where(etypetest==i)[0]
    if len(xtest[temp2,])>0:
         p = mod.predict(xtest[temp2,])
         yhats.append(p)
         mses.append(rmse(p,ytest[temp2]))
    else:
         yhats.append(np.nan)
         mses.append(np.nan)
    print(i)
print(time.time()-t1)

mses = pd.DataFrame(mses,np.ravel(etypetrains.values),['mses']).sort_index()

results = pd.concat([train.groupby([parval]).mean()['mme'],train.groupby([parval]).size(),mses,pd.DataFrame(data.iloc[yind]).groupby([parval]).size()],axis = 1).rename(columns = {'mme':'group mmemean avg',0:'group size',1:'total group size'})

#pickle.dump([mods,mses,yhats,results],open('data.pkl', 'wb'))
#mods,mses,yhats,results = pickle.load(open('data.pkl','rb'))

for i in range(len(etypetrains)):
     eid = i
     evalu = etypetrains.iloc[eid][0]
     p =yhats[eid]
     temp = np.where(etypetest==evalu)[0]
     
     fig = plt.figure()
     plt.get_current_fig_manager().window.showMaximized()
     plt.plot(ytest[temp],p,'.')
     plt.grid('on')
     plt.title(evalu)
     plt.xlabel('MME')
     plt.ylabel('Estimation')
#     plt.axis([0,max(ytest[temp]),0,max(p)])
     
     plt.figure()
     plt.get_current_fig_manager().window.showMaximized()
     plt.plot(np.percentile(np.abs(ytest[temp]-p)/ytest[temp],range(100)),range(100))
     plt.grid('on')
     plt.title(evalu)

# In[] LM
t1 = time.time()
xs = np.arange(0,px)
trace = np.zeros(0).astype(int)
for j in range(px):
    AICs = np.zeros(px)
    for i in range(px):
        if i in xs:
            xstemp = np.delete(xs,np.where(xs==i)[0])
        else:
            xstemp = np.append(xs,i)
        xtemp = xtrain[:,xstemp]
        AICs[i] = lm(xtemp,ytrain)[-2]
    AICs = np.append(AICs,lm(xtrain[:,xs],y)[-2])
    vind = np.where(AICs==min(AICs))[0][0]
    if vind in xs:
        trace = np.append(trace,-vind)
        xs = np.delete(xs,np.where(xs==vind)[0])
    elif vind == px:
        break
    else:
        trace = np.append(trace,vind)
        xs = np.append(xs,vind)
    print(trace)
    print(time.time()-t1)

mod1 = lm(xtrain[:,xs],y,xnames = xnames[xs])


# In[] evaluation
p1 = np.dot(xtest[:,xs],mod1[0])
mse1 = rmse(p1,ytest)

p2 = RF.predict(xtest)[:,None]
mse2 = rmse(p2,ytest)

plt.figure()
plt.get_current_fig_manager().window.showMaximized()
plt.barh(range(len(RFimpor)),RFimpor['importance'])
plt.yticks(ticks = range(len(RFimpor)),labels = np.array(RFimpor[0]))

plt.figure()
plt.get_current_fig_manager().window.showMaximized()
plt.hist((p2-ytest)/ytest,bins = 100)

plt.figure()
plt.get_current_fig_manager().window.showMaximized()
plt.scatter(ytest,p2)

# In[]
corr = data1.drop(['esn','mmemean']).corr()
plt.figure()
plt.get_current_fig_manager().window.showMaximized()
seaborn.heatmap(corr, center=0, annot=True, cmap='YlGnBu')
plt.show()

step = 10
avilratio = []
vnumratio = []
for i in np.linspace(10,n1,step):
    avilratio.append(sum(np.isnan(data1.iloc[:,np.where(np.isnan(data1).sum(0)<i)[0]]).sum(1)==0)/n1)
    vnumratio.append(np.sum(np.isnan(data1).sum(0)<i)/p1)
plt.figure()
plt.get_current_fig_manager().window.showMaximized()
plt.plot(np.linspace(10,n1,step),vnumratio,'-o')
plt.plot(np.linspace(10,n1,step),avilratio,'-o')
plt.title('data availability ratio by variable valid data threhold')
plt.legend(['ratio of valid vars','dataset availability'])
plt.grid('on')


plt.figure()
plt.get_current_fig_manager().window.showMaximized()
plt.hist(data['mmemean'],bins = 50)
plt.title('all data distribution')


etype = pd.get_dummies(data['engine_type'])
etypesum = etype.apply(lambda x: x.sum())
etype = np.array(etypesum.index,dtype = 'str')
etypemapping = {a:b for b,a in enumerate(set(etype))}
egroup = data.groupby('engine_type')
print(etypesum)
egroup['mmemean'].describe()

plt.figure()
plt.get_current_fig_manager().window.showMaximized()
plt.bar(range(len(etype)),etypesum)
plt.gca().set_xticks(range(len(etype)))
plt.gca().set_xticklabels(etype)
plt.title('data count by etype')
for tick in plt.gca().get_xticklabels():
    tick.set_rotation(30)

fig = plt.figure()
plt.get_current_fig_manager().window.showMaximized()
ax1 = fig.add_subplot(111)
ax1.bar(range(len(etype)),egroup.count()['mmemean'])
plt.legend(['valid data count'])
ax2 = ax1.twinx()
ax2.plot(range(len(etype)),egroup.count()['mmemean']/etypesum,'r')
plt.legend(['valid ratio'],bbox_to_anchor = [1,0.95],)
plt.title('valid data count and valid ratio by etype')
ax1.set_xticks(range(len(etype)))
ax1.set_xticklabels(etype)
for tick in ax1.get_xticklabels():
    tick.set_rotation(30)

data.boxplot(column = 'mmemean',by = 'engine_type')
plt.get_current_fig_manager().window.showMaximized()
plt.title('mme by enginetype')
for tick in plt.gca().get_xticklabels():
    tick.set_rotation(30)


validcount = []
for i in range(nesn):
    esn = esns[i]
    esndata = data.iloc[np.where(data['esn']==esn)[0]]
    m = len(esndata)
    validcount.append(len(esndata)-sum(np.isnan(esndata['mmemean'])))
validcount = np.array(validcount)
plt.figure()
plt.get_current_fig_manager().window.showMaximized()
plt.hist(validcount[validcount!=0],bins = 30)
plt.title('valid data count by esn WITHOUT 0s')
