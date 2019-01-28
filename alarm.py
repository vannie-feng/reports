#!/usr/bin/python2.7
#-*- coding:utf-8 -*-
import urllib
import urllib2
import requests
import json
from datetime import datetime,timedelta
import os
#import datetime.datetime
#import datetime.timedelta
# Read stats
#url = "http://10.18.75.167:9002/getState"
url="http://10.18.96.30:8920/stats"
'''
resp = requests.post(url)
#print (resp.text)
stats=json.loads(resp.text)
print stats
print stats.keys(url)
'''
def get_save(url):
    time=datetime.now().strftime('%Y-%m-%d %H:%M')
    print time
    resp = requests.post(url)
    stats=json.loads(resp.text)
    #save stats now
    with open('./out/'+time+'.txt','w') as f:
        w=json.dumps(stats,ensure_ascii=False)
        f.write(w)
        print "%s .txt' saved" % time
    return stats
def get_stat_30():
    #get stats 30minutes ago
    compare=datetime.now()-timedelta(days=1)
    for file in os.listdir('./out/'):
        name=file.split(".")[0]
        name=datetime.strptime(name,'%Y-%m-%d %H:%M')
        if (compare<name and name<datetime.now()-timedelta(minutes=1)):
            compare=name
#    a=datetime.now()-timedelta(minutes=20)
    a=compare
    time_30ago=a.strftime('%Y-%m-%d %H:%M')
    print "compare file:", time_30ago
    if not os.path.exists('./out/'+time_30ago+'.txt'):
        print "%s is not exists!" % time_30ago+'.txt'
        return False
    else:
        with open ('./out/'+time_30ago+'.txt') as f_30:
            stats_30 = f_30.readlines()
           # print stats_30[0]
          #  print type(stats_30[0])
            stats_30 = json.loads(stats_30[0])
    return stats_30

wait_check_list=[u'10.18.96.30_pgc',u'10.18.96.28_pgc',u'10.18.96.26',u'10.18.96.30',u'10.18.96.11',u'10.18.96.28',u'10.18.96.79',u'10.18.96.76']

# Compare results with json 30 minutes ago
def compare(stats,stats_30):
    data_diff=[]
    errorname=[]
    error_diff=[]
    ssh=['26','28','11','30','76','79']
    ssh_pgc=['30','28']
    count=0
    count_pgc=0
    dict_pro={} #记录各个机器今天待处理数据的个数
    sortlist=[]#记录各个机器今天待处理的个数，用于排序；
    sortlist_pgc=[]
    #compute process avg
    for k in ssh:
        keyname=eval("u'10.18.96.{}'".format(k))
        count+=stats[u'data'][keyname]
        sortlist.append(stats[u'data'][keyname])
        dict_pro[k]=stats[u'data'][keyname]
    avg=float(count)/6
    for k in ssh_pgc:
        keyname=eval("u'10.18.96.{}'".format(k))
        k=k+'_pgc'
        count_pgc+=stats[u'pgc_diff'][keyname]
        sortlist_pgc.append(stats[u'pgc_diff'][keyname])
        dict_pro[k]=stats[u'pgc_diff'][keyname]
        avg_pgc=float(count_pgc)/2
   # for kk,v in dict_pro.items():
       # print kk,v
    #====pending sorted====(append is to prevent index overflow)#
    sortlist.append(max(sortlist)+1)
    sortlist_pgc.append(max(sortlist_pgc)+1)
    sortlist.sort()
    sortlist_pgc.sort()
    for s in wait_check_list:
#       compute diff between now and 30minutes ago
      #  print "test print count:!!!!!!!!!!!!!!!!!!!! %s" %stats[u'today'][s][u'Receive'][u'Count']
        diff=stats[u'today'][s][u'Receive'][u'Count']-stats_30[u'today'][s][u'Receive'][u'Count']
        data_diff.append((s,diff))
        if diff==0:
            name1=s.split(".")[-1]
            print name1
            ispcg='pgc' in name1
            flag=0
            if(ispcg):
                avg_use=avg_pgc
                name=name1
               # name=name1.split("_")[0]
                sindex=sortlist_pgc.index(dic_pro[name])
                if (dic_pro[name]+50<sortlist_pgc[sindex+1]):
                    flag=1
            else:
                avg_use=avg
                name=name1
                sindex=sortlist.index(dict_pro[name])
                if (dict_pro[name]+50<sortlist[sindex+1]):
                    flag=1
            if (dict_pro[name]<avg_use)and(flag==1):
         #       print avg_use
                errorname.append("u'10.18.96.{}".format(name1))
                error_diff.append((dict_pro[name],diff))
    return errorname,error_diff

def testfun():
    
    stats=get_save(url)
    stats_30=get_stat_30()
    print "data_30 ago get!"
    errorname,pro_diff=compare(stats,stats_30)
    print "error!"
        #exit(0)
    if len(errorname)>0:
        for i in range(len(errorname)):
            msg = '{} consumer maybe dead, diff {}'.format(errorname[i],pro_diff[i])
            print msg
    else:
        print "OK!"
#compare(stats)    
# Send alarm



URL = r'http://service1.mrd.sohuno.com/mrdmsg/sms?'

def send_alarm():
    stats=get_save(url)
    stats_30=get_stat_30()
    print "data_30 ago get!"
    errorname,pro_diff=compare(stats,stats_30)
        #exit(0)
    msg=[]
    if len(errorname)>0:
        for i in range(len(errorname)):
            msg .append('{} consumer maybe dead, (data/pgc num:{},diff_30_ago:{}) '.format(errorname[i],pro_diff[i][0],pro_diff[i][1]))
            #print msg
        flag=1
    else:
        print "OK!"
        flag=0
    if flag:
        recipient='18811521363,17638598892'
        params = urllib.urlencode({'to': recipient, 'msg': msg})
        print urllib2.urlopen(URL, params).read()
    
if __name__=='__main__':
  #  testfun()
    send_alarm()
