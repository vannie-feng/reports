#!/usr/bin/python2.7
#-*- coding:utf-8 -*-
from email import sendEmail
import MySQLdb as mb
from datetime import datetime
import os
import json
import redis
import argparse
import numpy as np

parser = argparse.ArgumentParser()
parser.add_argument("--dev", "-d", action='store_true')
args = parser.parse_args()
if args.dev:
    dev = True
else:
    dev = False

DB_HOSTNAME = 'recom-qualityrw.db2.sohuno.com'
DB_PORT = 3306
DB_USERNAME = 'recom_quality_rw'
DB_PASSWORD = '8l9c6q8S74U79MA'
DB_NAME = 'recom_quality'
DB_CHARSET = 'utf8'

SQL_SELECT_HEADNEWS = "select " \
        "nid, oid " \
        "from t_headnews " \
        "where main_end_time > '%s' and " \
        "uptime < '%s' "
SQL_SELECT_EID_COUNT = "select " \
        "eid, count(*) " \
        "from t_eid_article " \
        "group by eid "
SQL_SELECT_EID_HEADNEWS = "select " \
        "t1.nid, t1.oid, t2.eid " \
        "from t_headnews t1, t_eid_article t2 " \
        "where t1.nid = t2.nid and " \
        "t1.main_end_time > '%s' and " \
        "t1.uptime < '%s' "

host = "sf.y.redis.sohucs.com"
port = 25008
passwd = "b83e99ea9a96b8377282ee0ec2d60311"

NEWS_URL_PRE = 'https://3g.k.sohu.com/t/n'


def proc_redis_data(host, port, passwd, all_nids, sub_nids):
#all_nids include headnews;
#sub_nids include headnews who has eid;
    pool = redis.ConnectionPool(host=host, password=passwd, port=port)
    r = redis.Redis(connection_pool=pool)
    now_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    # 要闻新闻的nid
    nids = r.keys()
    click, hle, hlr = [0, 0], [0, 0], [0.0, 0.0]
    sub = {}
    hlr_list=[]
    for nid in nids:
        
        content = r.hgetall(nid)
        if "oid" in content.keys():
            try:
                oid = content['oid']
                if nid in all_nids:
                    hle[0] += int(content['hle'])
                    hlr[0] += float(content['hlr'])
                    click[0] += (float(content['hlr']) * int(content['hle']))
                    hlr_list.append(float(content['hlr']))
                if nid in sub_nids:
                    hle[1] += int(content['hle'])
                    hlr[1] += float(content['hlr'])
                    click[1] += (float(content['hlr']) * int(content['hle']))
                if nid in all_nids and nid not in sub_nids:
                    sub[(nid, oid)] = (int(content['hle']), float(content['hlr']), float(content['hlr']) * int(content['hle']))
            except Exception:
                continue
    sub = sorted(sub.items(), lambda x, y: cmp(x[1][2], y[1][2]), reverse=True)
    print len(hlr_list)
#==========统计按照点击率分段排名的eid数占比===============#
    flag25,flag50,flag75= np.percentile(hlr_list, [25, 50, 75])
    print ("flags:",flag25,flag50,flag75)
    percencount={}#keys(25,50,75,100),items(all_count,eid_count)
    keylist=['25','50','75','100','200']
    for key1 in keylist:
        percencount[key1]=[0,0]
    for nid in nids:
        content = r.hgetall(nid)
   #     print content
        if "oid" in content.keys():
       #     print("11")
            try:
        #        print "here"
                keyname='200'
                oid = content['oid']
                if(float(content['hlr'])<flag25):
                    keyname='25'
                elif(float(content['hlr'])<flag50):
                    keyname='50'
                elif(float(content['hlr'])<flag75):
                    keyname='75'
                else:
                    keyname='100'
   #             print (keyname,float(content['hlr']))
                if nid in all_nids:
                    percencount[keyname][0]+=1
                if nid in sub_nids:
                    percencount[keyname][1]+=1
            except Exception:
                continue
    for k,v in percencount.items():
        print (k,v)
  #  exit(0)
    return hle, hlr, click, sub[:10], now_time,percencount




if __name__ == '__main__':
    
    f = open('_pid', 'w')
    f.write('%s' % os.getpid())
    f.close()

    conn = mb.connect(host=DB_HOSTNAME, port=DB_PORT, user=DB_USERNAME, passwd=DB_PASSWORD, db=DB_NAME, charset=DB_CHARSET)
    assert conn
    cur = conn.cursor()
    assert cur

    endtime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print endtime
    
    cur.execute(SQL_SELECT_HEADNEWS % (endtime, endtime))
    conn.commit()
    headnews_datas = cur.fetchall()
    print 'headnews_count : ', len(headnews_datas)

    cur.execute(SQL_SELECT_EID_COUNT)
    conn.commit()
    eid_count = cur.fetchall()
    print 'eid_count : ', len(eid_count)
    print 'eid_count > 3: ', len(filter(lambda x:x[1] > 3, eid_count))

    cur.execute(SQL_SELECT_EID_HEADNEWS % (endtime, endtime))
    conn.commit()
    eid_headnews_datas = cur.fetchall()
    print 'eid_headnews_count : ', len(eid_headnews_datas)

    all_nids = {nid:1 for nid in map(lambda x:x[0], headnews_datas)}
    sub_nids = {nid:1 for nid in map(lambda x:x[0], eid_headnews_datas)}
    hle, hlr, click, sub_datas, now_time, percencount= proc_redis_data(host, port, passwd, all_nids, sub_nids)
    print 'hle : ', hle
    print 'hlr : ', hlr
    print 'click : ', click
    print 'sub_datas : ', sub_datas
    print 'time:',now_time
    fw = open('message.txt', 'w')
    

    fw.write("<table width='500px' border='1'>")
    fw.write("<tr><th>Name</th><th>Value</th></tr>")
    fw.write("<tr><td align='center'>获取redis时间</td><td align='center'>%s</td></tr>" % now_time) 
    fw.write("<tr><td align='center'>当前要闻总数</td><td align='center'>%s</td></tr>" % len(headnews_datas)) 
    fw.write("<tr><td align='center'>当前eid总数</td><td align='center'>%s</td></tr>" % len(eid_count)) 
    fw.write("<tr><td align='center'>当前eid新闻个数>3总数</td><td align='center'>%s</td></tr>" % len(filter(lambda x:x[1] > 3, eid_count))) 
    fw.write("<tr><td align='center'>有eid的要闻个数</td><td align='center'>%s</td></tr>" % (len(eid_headnews_datas))) 
    
    proportion = 'n/a' if len(headnews_datas) == 0 else float(len(eid_headnews_datas)) / len(headnews_datas)
    fw.write("<tr><td align='center'>有eid的要闻个数比例</td><td align='center'>%s / %s = %.4f</td></tr>" % \
            (len(eid_headnews_datas), len(headnews_datas), proportion))
    
    proportion = 'n/a' if hle[0] == 0 else float(hle[1]) / hle[0]
    fw.write("<tr><td align='center'>曝光率比例</td><td align='center'>%s / %s = %.4f</td></tr>" % \
            (hle[1], hle[0], proportion))
    
    proportion = 'n/a' if hlr[0] == 0 else float(hlr[1]) / hlr[0]
    fw.write("<tr><td align='center'>点击率比例</td><td align='center'>%.4f / %.4f = %.4f</td></tr>" % \
            (hlr[1], hlr[0], proportion))
    
    proportion = 'n/a' if click[0] == 0 else float(click[1]) / click[0]
    fw.write("<tr><td align='center'>点击数比例</td><td align='center'>%.4f / %.f4 = %.4f</td></tr>" % \
            (click[1], click[0], proportion))

    fw.write("</table>")
    keylist=['100','75','50','25']
    for key1 in keylist:
       # if key1=='200':
        #    continue
        proportion=float(-999) if percencount[key1][0]==0 else float(percencount[key1][1])/percencount[key1][0]
        fw.write("<br/>点击率%s-%s分位数段eid占比：      </td><td align = 'center'>%s/%s = %.4f<br/>" %\
                (str(eval(key1)-25),key1,percencount[key1][1],percencount[key1][0],proportion))
    fw.write("<br/>没有eid的要闻点击比top10:<br/>")
    for one in sub_datas:
        
        (nid, oid), (hle, hlr, click) = one
        href = '%s%s' % (NEWS_URL_PRE, oid)
        fw.write(ur"<a href='%s' >%s</a><br/>" % (href, href))




    fw.close()
    cur.close()
    conn.close()
    if not sendEmail(dev):
        exit(1)
    
    
    print 'done'
    exit(0)
