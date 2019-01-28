#!/usr/bin/python2.7
# -*- coding: utf-8 -*-
import os,commands
from datetime import datetime
import datetime as dt
from multiprocessing import Pool
from email import sendEmail
import json
import argparse
import numpy as np
import pycurl
import MySQLdb as mb
'''

数据长这样：(sep[0][i])

0		 tid: 1328219
1		  id: 251155409025478656
2	 sim_id: NULL
3		 nid: NULL
4		 oid: NULL
5		  ts: 2018-11-25 23:59:59
6	platform: "\u5317\u7814-\u897f\u74dc"
7	   ctime: NULL
8		type: 1
9		 url: http://kimg01.youju.sohu.com/fbec1016-02fa-4a7c-8864-0bd0a186e100.jpeg
10 headpic_url: NULL
11  dedup_only: Y
12	  is_pgc: N
13   sim_video: NULL
14   play_time: 110.0000
15 elapsed_time: NULL
16		 ip: 10.18.96.28
 17	raw_msg: {"pic": "http://kimg01.youju.sohu.com/fbec1016-02fa-4a7c-8864-0bd0a186e100.jpeg", "ifCrawl2Pgc": false, "ip": "10.18.96.28", "sendTs": 1543161599941, "plat": "\u5317\u7814-\u897f\u74dc", "pubTime": "2018-11-24 21:22:24", "dedupOnly": true, "oldVUrl": "https://3g.k.sohu.com/t/n323264696", "oldVDownloadUrl": "http://kvideo01.youju.sohu.com/09f7d0b5-d841-495f-9108-8662c38fef092_0_0.mp4", "id": "251155409025478656", "description": "\u5988\u5988\u5411\u524d\u51b2 \u6c6a\u6674\u6b32\u8d74\u7f8e\u63a2\u671b\u75c5\u91cd\u7684\u51ef\u6587\uff0c\u5374\u672a\u80fd\u6210\u884c.\u524d\u592b\u5251\u5e73\u85c9\u6b64\u60f3\u548c\u6c6a\u6674\u590d\u5a5a\uff0c\u4e0d\u6599\u80e1\u8389\u62b1\u7740\u5c0f\u5b69\u56de\u6765\u5c39\u5bb6\u8ba4\u7956\u5f52\u5b97.\u5176\u5b9e\u771f\u80e1\u8389\u5df2\u96be\u4ea7\u8fc7\u4e16.\u80ce\u59b9\u838e\u838e\u5192\u5145\u80e1\u8389\u524d\u6765\u5c39\u5bb6\u539f\u672c\u662f\u60f3\u8981\u4e00\u7b14\u629a\u517b\u8d39\u5c31\u8d70\u4eba.\u4f46\u5979\u53d1\u73b0\u5251\u5e73\u662f\u5bb6IT\u516c\u53f8\u7684\u8001\u677f\u8d77\u4e86\u90aa\u5ff5.", "category": "1013", "job_id": "1543161600_9712", "title": "\u4eb2\u5988\u7ed9\u5973\u513f\u6d17\u6fa1\u770b\u5230\u624b\u4e0a\u80bf\u4e86\uff0c\u5973\u513f\u4e00\u8138\u59d4\u5c48\u8bf4\uff1a\u540e\u5988\uff0c\u4eb2\u5988\u5f88\u5fc3\u75bc", "media": "\u9999\u8549\u5267\u4e2d\u5178", "ts": 1543161599941, "location": "/data/crawlvideo/251155409025478656.mp4", "playTime": "110.0", "fileSizeHigh": 7410312}
18 summary_info: NULL
19   createtime: 2018-11-26 00:00:00
20	  uptime: 2018-11-26 00:00:00
21   update_by: kafka_to_bst.py
22		vid: NULL
)
'''
start_time = '2018-12-10 00:00:00'
end_time = '2018-12-11 00:00:00'

PRINT_COUNT = 200

parser = argparse.ArgumentParser()
parser.add_argument("--dev", "-d", action='store_true')
args = parser.parse_args()
#判断是否是dev
if args.dev:
	dev = True
else:
	dev = False
	start_time = None
	end_time = None

end_date = datetime.now().date()
if end_time == None:
	end_time = end_date
	end_time = str(end_time) + ' 00:00:00'
if start_time == None:
	endtime = datetime.strptime(str(end_time), "%Y-%m-%d %H:%M:%S")
	start_time = endtime-dt.timedelta(days=1)
	start_time = str(start_time) #+ ' 00:00:00'

stime = datetime.strptime(str(start_time), "%Y-%m-%d %H:%M:%S")
dtime = datetime.strptime(str(end_time), "%Y-%m-%d %H:%M:%S")

video_type = [ur'西瓜视频', ur'爆米花视频', ur'好看视频', ur'梨视频', ur'美拍视频']
print stime, dtime

HOST_DB = 'recom-qualityrw.db2.sohuno.com'
PORT_DB = 3306
USER_DB = 'recom_quality_rw'
PASSWD_DB = '8l9c6q8S74U79MA'
DB ='recom_quality'
CHARSET = 'utf8'






def proc_kafka_sql(cur, SQL_QUERY):

#处理接收日志的函数
#目标是各平台视频计数、合计、北研计数（11）
#返回各平台视频计数字典和北研计数；
	ids_pgc = set()#统计去重
	total_count=0
	total_count_pgc=0
	
	dedupcount=0
	dedupcount_pgc=0
	cur.execute(SQL_QUERY, None)
	res = cur.fetchall()
	print len(res)#(项目数,23)
	video_type_dict = {}
	video_type_dict_pgc = {}
	for type1 in video_type:
		video_type_dict[type1]=0
		video_type_dict_pgc[type1]=0
	ids = []
	
	for item in res:
		#item是每一项，有23个维度；
               # print item[12]
		if item[12]=="N":#非pgc
	            total_count+=1
		    try:
		        plat = json.loads(item[6])

                        if plat not in video_type_dict:
                            pass
                        else:
                            video_type_dict[plat] += 1
		    except Exception, e:
                        print e
			continue
                    ids.append(item[1])
		    if item[11]=="Y":
                        dedupcount+=1
				
		elif item[12]=="Y":
			total_count_pgc+=1
			ids_pgc.add(item[1])
			try:
			    plat = json.loads(item[6])
			    if plat not in video_type_dict_pgc:
                                pass
                            else:
                                video_type_dict_pgc[plat] += 1
			except Exception, e:
                                print e
				continue
			if item[11]=="Y":
				dedupcount_pgc+=1
	id_receive_count_pgc=len(ids_pgc)
        dictkey=['video_type_dict', 'video_type_dict_pgc','dedupcount','dedupcount_pgc','id_receive_count_pgc','total_count','total_count_pgc','ids']
        receive_dict={}
        for key in dictkey:
            receive_dict[key]=eval(key)
	return receive_dict
def proc_process_sql(cur, SQL_QUERY):

	#处理发送文件的
	
	total_time = 0	#总处理时长
	total_time_pgc=0
	total_time_count = 0#
	total_time_count_pgc=0
	
	has_video_out_count = 0
	has_headpic_url_count = 0
	
	sim_count=0			#有sim字段的个数
	sim_count_pgc=0
	has_sim_video_dict = {}#有sim字段的分平台统计
	
	job_ids = set()#有sim字段记录job_ids
	id_pgc=set()#for 去重
	id_sim_pgc=set()#for sim去重
	sim_video_list_pgc=[]

	
        #北研数据
	dedupcount=0	#北研数据
        dedup_total_time=0
	dedupcount_pgc=0
        #播放时间
        playtime=0
        playtime_dedup=0
	
	cur.execute(SQL_QUERY, None)
	res = cur.fetchall()
        no_video_out=set()
        video_type_dict = {}
        has_sim_video_dict={}
	for typename in video_type:
		video_type_dict[typename]=0
                has_sim_video_dict[typename]=0
        ids=[]

	for item in res:
		#item是每一项，有23个维度；
		if item[12]=="N":#非pgc
                        ids.append(item[1])
			try:
                            #plat = item[6]
			    plat = json.loads(item[6])
			except Exception, e:
			    print e
                            continue
			if item[11]=="Y":
				dedupcount+=1
				dedup_total_time+=float(item[15])
                                if item[14]:
                                    playtime_dedup+=float(item[14])
                        else:
                                if item[14]:
                                    playtime+=float(item[14])
			total_time_count += 1
			temp = item[15]
			total_time += float(temp)
                        
                        temp_17=json.loads(item[17])
			#sim_video:统计有字段的个数，记录其job_ids；
                        if  (len(item[13])>2):
				sim_count+=1
				job_id = temp_17['job_id']
				job_ids.add(job_id)
                                if plat not in video_type_dict:
			            pass
                                else:
                                    video_type_dict[plat] += 1
                        if u'video_out' not in temp_17.keys():
                            no_video_out.add(item[1])
                        elif len(temp_17[u'video_out']) > 0:
				has_video_out_count += 1				
                        #headpic_url_count   
			if len(item[10]) > 0:
				has_headpic_url_count += 1
                        	
		     #   if item[14]:
                    #        playtime+=float(item[14])
		elif item[12]=="Y":#pgc

			#统计去重
			id_pgc.add(item[1])
			total_time_count_pgc += 1
			temp = item[15]
			total_time_pgc += float(temp)
	
			#sim_video:统计有字段的个数；pcg记录字段
                        if (len(item[13])>2):
				id_sim_pgc.add(item[1])
				sim_count_pgc+=1
				try:
                                    plat = json.loads(item[6])
				except Exception, e:
				    continue
				if plat not in has_sim_video_dict:
#					has_sim_video_dict[plat] = 0
                                    pass
                                else:	
                                    has_sim_video_dict[plat] += 1
				sim_video_list_pgc.append(item[13])
			#
								

	avg_time = (total_time / total_time_count) if total_time_count > 0 else 0
        avg_dedup = (dedup_total_time/dedupcount) if dedupcount>0 else 0
        avg_nodedup = (total_time-dedup_total_time)/(total_time_count-dedupcount) if (total_time_count-dedupcount)>0 else 0
	avg_playtime=playtime/(total_time_count-dedupcount) if (total_time_count-dedupcount)>0 else 0
	avg_playtime_dedup=playtime_dedup/dedupcount if dedupcount>0 else 0
	
	id_send_count_pgc=len(id_pgc)#去重后处理视频总个数
	id_send_sim_count_pgc=len(id_sim_pgc)#去重后有sim字段；

##      make return dict
        dictkeys=['video_type_dict','avg_time','dedupcount','total_time_count','sim_count','has_video_out_count','has_headpic_url_count','job_ids','total_time','total_time_count_pgc','id_send_count_pgc','total_time_pgc','sim_count_pgc','id_send_sim_count_pgc','has_sim_video_dict','sim_video_list_pgc','avg_dedup','avg_nodedup','avg_playtime','avg_playtime_dedup','ids']        
        send_dict={}
        for key in dictkeys:
            send_dict[key]=eval(key)
        
        return send_dict
        

if __name__ == '__main__':
	
        FOUT = './out.txt'
        
       # hosts=['26','28','30']
	hosts=['26','28','30','11','76','79']
        data_dict_receive={'26':{},'28':{},'30':{},'11':{},'76':{},'79':{}}
        data_dict_send={'26':{},'28':{},'30':{},'11':{},'76':{},'79':{}}
	
	conn = mb.connect(host=HOST_DB, port=PORT_DB, user=USER_DB, passwd=PASSWD_DB, db=DB, charset=CHARSET)
        assert conn
	cur = conn.cursor()
	assert cur	
	for host in hosts:
		##set cur conn#
		SQL_SELECT_RECEIVE ="select * from t_video_oscar where ip ='10.18.96.{}' and type=1 and "\
			"createtime between '{}' and '{}'".format(host,stime, dtime)
		SQL_SELECT_SEND = "select * from t_video_oscar where ip ='10.18.96.{}' and type=2 and "\
			"createtime between '{}' and '{}'".format(host,stime, dtime)
                print  SQL_SELECT_RECEIVE
		##get data##
                data_dict_receive[host]=proc_kafka_sql(cur,SQL_QUERY=SQL_SELECT_RECEIVE)
                data_dict_send[host]=proc_process_sql(cur, SQL_QUERY=SQL_SELECT_SEND)
        cur.close()
	conn.close()


        #make table#
        f=open(FOUT,'w')
	f.write("<table border='1' style='width:800px'>")
	f.write("<tr><th>type</th><th>总和</th><th>10.18.96.30</th><th>10.18.96.26</th><th>10.18.96.28</th><th>10.18.96.11</th><th>10.18.96.76</th><th>10.18.96.79</th></tr>")
	
	num_30 = '{} {}'.format(stime, dtime) 
	num_26 = '{} {}'.format(stime, dtime)
	num_28 = '{} {}'.format(stime, dtime) 
        num_11 = '{} {}'.format(stime, dtime)
        num_76 = '{} {}'.format(stime, dtime)
        num_79 = '{} {}'.format(stime, dtime)

        f.write("<tr><td>数据时间</td><td></td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>".\
                format(num_30, num_26, num_28,num_11,num_76,num_79))
	
       # print data_dict_receive['30'].keys()
	num_30 = data_dict_receive['30']['total_count']
	num_26 = data_dict_receive['26']['total_count'] 
        num_28 = data_dict_receive['28']['total_count'] 
        num_11 = data_dict_receive['11']['total_count'] 
        num_76 = data_dict_receive['76']['total_count'] 
        num_79 = data_dict_receive['79']['total_count'] 
        total = num_30 + num_26 + num_28+num_11+num_76+num_79
	f.write('<tr><td>接收视频总个数</td><td><font color="#3300FF">{}</font></td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>'.\
			format(total, num_30, num_26, num_28,num_11,num_76,num_79))

	for vtype in video_type:
		
		num_30 = data_dict_receive['30']['video_type_dict'][vtype] if vtype in data_dict_receive['30']['video_type_dict'] else 0
		num_28 = data_dict_receive['28']['video_type_dict'][vtype] if vtype in data_dict_receive['28']['video_type_dict'] else 0		
		num_26 = data_dict_receive['26']['video_type_dict'][vtype] if vtype in data_dict_receive['26']['video_type_dict'] else 0
		num_11 = data_dict_receive['11']['video_type_dict'][vtype] if vtype in data_dict_receive['11']['video_type_dict'] else 0
		num_76 = data_dict_receive['76']['video_type_dict'][vtype] if vtype in data_dict_receive['76']['video_type_dict'] else 0
                num_79 = data_dict_receive['79']['video_type_dict'][vtype] if vtype in data_dict_receive['79']['video_type_dict'] else 0
		total = num_30 + num_26 + num_28+num_11+num_76+num_79
		f.write('<tr bgcolor="#C0C0C0"><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>'.\
                        format(vtype, total, num_30, num_26, num_28,num_11,num_76,num_79))
   
	num_30 = data_dict_receive['30']['dedupcount']
	num_26 = data_dict_receive['26']['dedupcount']
	num_28 = data_dict_receive['28']['dedupcount']
	num_11 = data_dict_receive['11']['dedupcount']
	num_76 = data_dict_receive['76']['dedupcount']
	num_79 = data_dict_receive['79']['dedupcount']
	total = num_30 + num_26 + num_28+num_11+num_76+num_79
	f.write('<tr><td>接收北研视频个数</td><td><font color="#3300FF">{}</font></td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>'.\
			format(total, num_30, num_26, num_28,num_11,num_76,num_79))
	
	#我是分割线 
	f.write('<tr><td align="center">------</td><td align="center">------</td><td align="center">------</td><td align="center">------</td><td align="center">------</td>\
		<td align="center">------</td><td align="center">------</td><td align="center">------</td></tr>')


	num_30 = data_dict_send['30']['total_time_count']
	num_26 = data_dict_send['26']['total_time_count']
	num_28 = data_dict_send['28']['total_time_count']
	num_11 = data_dict_send['11']['total_time_count']
	num_76 = data_dict_send['76']['total_time_count']
	num_79 = data_dict_send['79']['total_time_count']
	total = num_30 + num_26 + num_28+num_11+num_76+num_79
	f.write('<tr><td>处理视频总个数</td><td><font color="#3300FF">{}</font></td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>'.\
			format(total, num_30, num_26, num_28,num_11,num_76,num_79))
	total_count=total

	num_30 = data_dict_send['30']['sim_count']
	num_26 = data_dict_send['26']['sim_count']
	num_28 = data_dict_send['28']['sim_count']
	num_11 = data_dict_send['11']['sim_count']
	num_76 = data_dict_send['76']['sim_count']
	num_79 = data_dict_send['79']['sim_count']
	total = num_30 + num_26 + num_28+num_11+num_76+num_79
	f.write('<tr><td>有sim_video字段的个数</td><td><font color="#3300FF">{}</font></td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>'.\
			format(total, num_30, num_26, num_28,num_11,num_76,num_79))

	for vtype in video_type:
		
		num_30 = data_dict_send['30']['video_type_dict'][vtype] if vtype in data_dict_send['30']['video_type_dict'] else 0
		num_26 = data_dict_send['26']['video_type_dict'][vtype] if vtype in data_dict_send['26']['video_type_dict'] else 0
		num_28 = data_dict_send['28']['video_type_dict'][vtype] if vtype in data_dict_send['28']['video_type_dict'] else 0
		num_11 = data_dict_send['11']['video_type_dict'][vtype] if vtype in data_dict_send['11']['video_type_dict'] else 0
		num_76 = data_dict_send['76']['video_type_dict'][vtype] if vtype in data_dict_send['76']['video_type_dict'] else 0
                num_79 = data_dict_send['79']['video_type_dict'][vtype] if vtype in data_dict_send['79']['video_type_dict'] else 0
		total = num_30 + num_26 + num_28+num_11+num_76+num_79
		f.write('<tr bgcolor="#C0C0C0"><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>'.\
				format(vtype, total, num_30, num_26, num_28,num_11,num_76,num_79)) 

	
	num_30 = data_dict_send['30']['avg_time'] 
	num_26 = data_dict_send['26']['avg_time']
	num_28 = data_dict_send['28']['avg_time']
	num_11 = data_dict_send['11']['avg_time']
	num_76 = data_dict_send['76']['avg_time']
	num_79 = data_dict_send['79']['avg_time']
	total = sum(map(lambda x:x[1]['total_time'],data_dict_send.items()))/\
	sum(map(lambda x:x[1]['total_time_count'],data_dict_send.items()))
	f.write('<tr><td>avg_proc_time</td><td><font color="#3300FF">{}s</font></td><td>{}s</td><td>{}s</td><td>{}s</td><td>{}s</td><td>{}s</td><td>{}s</td></tr>'.\
			format(total, num_30, num_26, num_28,num_11,num_76,num_79))

	num_30 = data_dict_send['30']['has_video_out_count'] 
	num_26 = data_dict_send['26']['has_video_out_count'] 
	num_28 = data_dict_send['28']['has_video_out_count'] 
	num_11 = data_dict_send['11']['has_video_out_count'] 
	num_76 = data_dict_send['76']['has_video_out_count'] 
	num_79 = data_dict_send['79']['has_video_out_count'] 
	total = sum(map(lambda x:x[1]['has_video_out_count'], data_dict_send.items()))
	f.write('<tr><td>has_video_out_count</td><td><font color="#3300FF">{}</font></td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>'.\
			format(total, num_30, num_26, num_28,num_11,num_76,num_79))
	
	num_30 = data_dict_send['30']['has_headpic_url_count'] 
	num_26 = data_dict_send['26']['has_headpic_url_count'] 
	num_28 = data_dict_send['28']['has_headpic_url_count'] 
	num_11 = data_dict_send['11']['has_headpic_url_count'] 
	num_76 = data_dict_send['76']['has_headpic_url_count'] 
	num_79 = data_dict_send['79']['has_headpic_url_count'] 
	total = sum(map(lambda x:x[1]['has_headpic_url_count'], data_dict_send.items()))
	f.write('<tr><td>headpic_url_count</td><td><font color="#3300FF">{}</font></td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>'.\
			format(total, num_30, num_26, num_28,num_11,num_76,num_79))
	
	num_30 = data_dict_send['30']['dedupcount'] 
	num_26 = data_dict_send['26']['dedupcount']
	num_28 = data_dict_send['28']['dedupcount'] 
	num_11 = data_dict_send['11']['dedupcount'] 
	num_76 = data_dict_send['76']['dedupcount'] 
	num_79 = data_dict_send['79']['dedupcount'] 
	total = num_30 + num_26 + num_28+num_11+num_76+num_79
	f.write('<tr><td>北研(不去水印)</td><td><font color="#3300FF">{}</font></td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>'.\
        format(total, num_30, num_26, num_28,num_11,num_76,num_79))
	dedupcount=total

	num_30 = data_dict_send['30']['avg_nodedup'] 
	num_26 = data_dict_send['26']['avg_nodedup']
	num_28 = data_dict_send['28']['avg_nodedup'] 
	num_11 = data_dict_send['11']['avg_nodedup'] 
	num_76 = data_dict_send['76']['avg_nodedup'] 
	num_79 = data_dict_send['79']['avg_nodedup'] 
        total = sum(map(lambda x:x[1]['avg_nodedup']*(x[1]['total_time_count']-x[1]['dedupcount']),data_dict_send.items()))/\
                (total_count-dedupcount) if (total_count-dedupcount)>0 else 0
	f.write('<tr><td>非北研平均处理时间</td><td><font color="#3300FF">{}</font></td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>'.\
			format(total, num_30, num_26, num_28,num_11,num_76,num_79))
	

	num_30 = data_dict_send['30']['avg_dedup'] 
	num_26 = data_dict_send['26']['avg_dedup']
	num_28 = data_dict_send['28']['avg_dedup'] 
	num_11 = data_dict_send['11']['avg_dedup'] 
	num_76 = data_dict_send['76']['avg_dedup'] 
	num_79 = data_dict_send['79']['avg_dedup'] 
        total = sum(map(lambda x:x[1]['avg_dedup']*x[1]['dedupcount'],data_dict_send.items()))/dedupcount if dedupcount >0 else 0
        f.write('<tr><td>北研平均处理时间</td><td><font color="#3300FF">{}</font></td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>'.\
                format(total,num_30,num_26,num_28,num_11,num_76,num_79))
        num_30 = data_dict_send['30']['avg_playtime'] 
        num_26 = data_dict_send['26']['avg_playtime']
        num_28 = data_dict_send['28']['avg_playtime'] 
	num_11 = data_dict_send['11']['avg_playtime'] 
	num_76 = data_dict_send['76']['avg_playtime'] 
	num_79 = data_dict_send['79']['avg_playtime'] 
        total = sum(map(lambda x:x[1]['avg_playtime']*(x[1]['total_time_count']-x[1]['dedupcount']),data_dict_send.items()))/(total_count-dedupcount)\
                if (total_count-dedupcount)>0 else 0
        f.write('<tr><td>非北研视频播放平均时长</td><td><font color="#3300FF">{}</font></td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>'.\
              format(total,num_30,num_26,num_28,num_11,num_76,num_79))
        num_30 = data_dict_send['30']['avg_playtime_dedup'] 
        num_26 = data_dict_send['26']['avg_playtime_dedup']
        num_28 = data_dict_send['28']['avg_playtime_dedup'] 
	num_11 = data_dict_send['11']['avg_playtime_dedup'] 
	num_76 = data_dict_send['76']['avg_playtime_dedup'] 
	num_79 = data_dict_send['79']['avg_playtime_dedup'] 
        total = sum(map(lambda x:x[1]['avg_playtime_dedup']*x[1]['dedupcount'],data_dict_send.items()))/dedupcount \
                if dedupcount>0 else 0
        f.write('<tr><td>北研视频播放平均时长</td><td><font color="#3300FF">{}</font></td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>'.\
              format(total,num_30,num_26,num_28,num_11,num_76,num_79))

	f.write('</table>')
	job_ids = data_dict_send['30']['job_ids']|data_dict_send['28']['job_ids']|data_dict_send['26']['job_ids']|data_dict_send['11']['job_ids']|data_dict_send['79']['job_ids']|data_dict_send['76']['job_ids']
	f.write("<font color='#FF0000'>有sim_video字段的job_ids:</font>")
	f.write('<br/>{}'.format(json.dumps(list(job_ids)[:PRINT_COUNT])))
        diff=set(data_dict_receive['30']['ids'])-set(data_dict_send['30']['ids'])
	f.write("<br/><font color='#FF0000'>receive-send 差集前200:</font>")
        f.write('<br/>{}'.format(json.dumps(list(diff)[:200])))
	

	#### PGC ###
	

        f.write('<br/>')
        f.write('<br/>')
        f.write('{}-{}<br/>'.format(stime, dtime))
        f.write("<font color='#FF0000'>PGC INFO:</font>")
        f.write("<table border='1' style='width:600px'>")
        f.write("<tr><th>type</th><th>总和</th><th>10.18.96.30</th><th>10.18.96.28</th></tr>")
        
        num_11=data_dict_receive['28']['total_count_pgc']
        num_30=data_dict_receive['30']['total_count_pgc']
        total_count_pgc=sum(map(lambda x:x[1]['total_count_pgc'],data_dict_receive.items()))
        f.write('<tr><td width = 300px>接收视频总个数</td><td><font color="#3300FF">{}</font></td><td><font color="#3300FF">{}</font></td><td><font color="#3300FF">{}</font></td></tr>'.format(total_count_pgc,num_30,num_11))
            
        num_11=data_dict_receive['28']['id_receive_count_pgc']
        num_30=data_dict_receive['30']['id_receive_count_pgc']
        total_count_pgc_rd=sum(map(lambda x:x[1]['id_receive_count_pgc'],data_dict_receive.items()))
        f.write('<tr><td width = 300px>去重后接收视频总个数</td><td><font color="#3300FF">{}</font></td><td><font color="#3300FF">{}</font></td><td><font color="#3300FF">{}</font></td></tr>'.format(total_count_pgc_rd,num_30,num_11))
            
        for one in video_type:
            video_type_dict_pgc=sum(map(lambda x:x[1]['video_type_dict_pgc'][one],data_dict_receive.items()))
            num_11=data_dict_receive['28']['video_type_dict_pgc'][one]
            num_30=data_dict_receive['30']['video_type_dict_pgc'][one]
            f.write("<tr bgcolor=#ADADAD><td align='center'>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>".format(one,video_type_dict_pgc,num_30,num_11))
        f.write('</table>')
#====================== receive-send line======================	
	f.write('<br/>')
	#received_ids = ids
	f.write("<table border='1' style='width:600px'>")

        num_11=data_dict_send['28']['total_time_count_pgc']
        num_30=data_dict_send['30']['total_time_count_pgc']
	total_count_pgc=sum(map(lambda x:x[1]['total_time_count_pgc'],data_dict_send.items()))
	f.write('<tr><td width = 300px>处理视频总个数</td><td><font color="#3300FF">{}</font></td><td><font color="#3300FF">{}</font></td><td><font color="#3300FF">{}</font></td></tr>'.format(total_count_pgc,num_30,num_11))
	
        num_11=data_dict_send['28']['id_send_count_pgc']
        num_30=data_dict_send['30']['id_send_count_pgc']
	total_count_pgc_rd=sum(map(lambda x:x[1]['id_send_count_pgc'],data_dict_send.items()))
	f.write('<tr><td width = 300px>去重后处理视频总个数</td><td><font color="#3300FF">{}</font></td><td><font color="#3300FF">{}</font></td><td><font color="#3300FF">{}</font></td></tr>'.format(total_count_pgc_rd,num_30,num_11))
	
        num_11=data_dict_send['28']['total_time_pgc']/data_dict_send['28']['total_time_count_pgc'] if data_dict_send['28']['total_time_count_pgc']>0 else 0
        num_30=data_dict_send['30']['total_time_pgc']/data_dict_send['30']['total_time_count_pgc'] if data_dict_send['30']['total_time_count_pgc'] else 0
        avg_time_pgc=sum(map(lambda x:x[1]['total_time_pgc'],data_dict_send.items()))/\
	sum(map(lambda x:x[1]['total_time_count_pgc'],data_dict_send.items()))
	f.write('<tr><td>平均处理时间</td><td>{} 秒</td><td>{} 秒</td><td>{} 秒</td></tr>'.format(avg_time_pgc,num_30,num_11))
	
        
        num_11=data_dict_send['28']['sim_count_pgc']
        num_30=data_dict_send['30']['sim_count_pgc']
	sim_count_pgc=sum(map(lambda x:x[1]['sim_count_pgc'],data_dict_send.items()))
	f.write('<tr><td>有sim_video字段的个数</td><td><font color="#3300FF">{}</font></td><td><font color="#3300FF">{}</font></td><td><font color="#3300FF">{}</font></td></tr>'.format(sim_count_pgc,num_30,num_11))
        
        num_11=data_dict_send['28']['id_send_sim_count_pgc']
        num_30=data_dict_send['30']['id_send_sim_count_pgc']
	sim_count_pgc_rd=sum(map(lambda x:x[1]['id_send_sim_count_pgc'],data_dict_send.items()))
	f.write('<tr><td>去重后sim_video字段个数</td><td><font color="#3300FF">{}</font></td><td><font color="#3300FF">{}</font></td><td><font color="#3300FF">{}</font></td></tr>'.format(sim_count_pgc_rd,num_30,num_11))
        for one in video_type:
  #              data_dict_send['26']['has_sim_video_dict'][one]
            video_type_dict_pgc=sum(map(lambda x:x[1]['has_sim_video_dict'][one],data_dict_send.items()))
            num_11=data_dict_send['28']['has_sim_video_dict'][one]
            num_30=data_dict_send['30']['has_sim_video_dict'][one]
            f.write("<tr bgcolor=#ADADAD><td align='center'>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>".format(one, video_type_dict_pgc,num_30,num_11))
        f.write('</table>')
        

	f.write('<br/>')#空一行
	
        f.write("<font color='#FF0000'>sim_video:</font>")
	sim_video=[]
	for host in hosts:
                if len(data_dict_send[host]['sim_video_list_pgc'])>0:
		    sim_video=list(set(data_dict_send[host]['sim_video_list_pgc']+sim_video))
	f.write('<br/>{}'.format(json.dumps(sim_video[:PRINT_COUNT])))

	f.close()

	sendEmail(dev)	
	#sendEmail(True)	
	
	print 'end'
        exit(0)
