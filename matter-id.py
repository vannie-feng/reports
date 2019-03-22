#!/usr/bin/python2.7
# -*- coding: UTF-8 -*-
import os,commands
from datetime import datetime
import datetime as dt
from sql import SQL
from multiprocessing import Pool
from email import sendEmail
import json
import sys
reload(sys)
sys.setdefaultencoding("utf-8")
import argparse
import random
PATH="./logs/"

sql_host ='recom-qualityrw.db2.sohuno.com'
sql_port = 3306
sql_username = 'recom_quality_rw'
sql_password = '8l9c6q8S74U79MA'
sql_dbname = 'recom_quality'

CASE_COUNT = 100

start_time='2018-11-28 00:00:00'
end_time='2018-11-28 14:00:00'
#start_time = None
#end_time = None

TABLE_NAME = 't_pick_image'
#TABLE_NAME = 't_list_image_score_dev'

parser = argparse.ArgumentParser()
parser.add_argument("--dev", "-d", action='store_true')
parser.add_argument("--createtime", "-c", action='store_true')
parser.add_argument("--ss","-s",nargs='?',type=str)
args = parser.parse_args()

def set_time():
    if len(sys.argv)<3:
        end_time=datetime.now().date()
        start_time=end_time-dt.timedelta(days=1)
    else:
        start_time=sys.argv[1]
        end_time=sys.argv[2]
    return start_time,end_time
if args.dev:
    dev = True
    TABLE_NAME = 't_list_image_score_dev'
    TABLE_NAME = 't_pick_image'
else:
    dev = False
    start_time = None
    end_time = None

COLUMN_NAME = 'createtime' if args.createtime else 'uptime'
sstime=None
if args.ss:
    #set_time()
    print ("here")
    sstime=args.ss

end_date = datetime.now().date()
if end_time == None:
    end_time = end_date
    end_time = str(end_time) + ' 00:00:00'
if start_time == None:
    endtime = datetime.strptime(str(end_time), "%Y-%m-%d %H:%M:%S")
    start_time = endtime-dt.timedelta(days=1)
    start_time = str(start_time) #+ ' 00:00:00'

if sstime==None:
    sstime=start_time
    setime=end_time
else:
    sstime = datetime.strptime(str(sstime), "%Y-%m-%d %H:%M:%S")
    setime=sstime+dt.timedelta(days=1)
    sstime=str(sstime)
    setime=str(setime)
def get_summary_data(filename,query_stime=sstime,query_etime=setime):

    with open(PATH+filename,'r') as f:
        cnt=1
        summary=0
        found=0
        changed=0
        line=f.readline()
    
        while line:
            try:
                logtime=line.split(",")[0]
                logtime=datetime.strptime(str(logtime), "%Y-%m-%d %H:%M:%S")
                query_stime=datetime.strptime(str(query_stime), "%Y-%m-%d %H:%M:%S")
                query_etime=datetime.strptime(str(query_etime), "%Y-%m-%d %H:%M:%S")
                if logtime<query_stime or logtime>query_etime:
                    line=f.readline()
                   # print ("logtime is %s" %str(logtime))
                    continue
                if 'SUMMARY' in line:
                    summary+=1
                if (line.find("found:")!=-1):
                    nf=line.index('found:')
                    strfound=line[nf:].split(',')[0].split(":")[1]
                    found+=(strfound == "True")or(strfound == " True")
                if (line.find("changed:")!=-1):
                    nc=line.index('changed:')
                    strchanged=line[nc:].split(',')[0].split(":")[1]
                    changed+=(strchanged == "True")or(strchanged == " True")
            except:
                print ("%d line has error,the line content is %s"% (cnt,line))
            line=f.readline()
            cnt+=1
    return summary,found,changed


sstime = datetime.strptime(str(sstime), "%Y-%m-%d %H:%M:%S")
stime = datetime.strptime(str(start_time), "%Y-%m-%d %H:%M:%S")
dtime = datetime.strptime(str(end_time), "%Y-%m-%d %H:%M:%S")
LOG_PATH = './shell_stats/'
log_count_max = 200

down_error_max = 0.02
interval_min = 60
down_error_times = []
down_error_stime = dtime
down_error_temp_count = 0
temp_pic_count = 0

not_match_nids = ['demo_nid', 'get_crop_nid']
if __name__ == '__main__':
    
    article_no_pic_count = 0L
    article_has_pic_count = 0L
    total_bug_number = 0L
    total_network_number = 0L
    bad_non_cdn_count = 0L
    good_down_count = 0L
    bad_down_count = 0L
    bad_upload_img = 0L
    bad_bg_img = 0L
    bad_size_count = 0L
    total_pics_count = 0L
    total_pic_body_count = 0L
    total_pic_full_count = 0L
    log_sent_kafka_count = 0L
    log_not_sent_kafka_count = 0L
    total_count = 0L
    #sim_pic_count = 0L
    sim_pic_article_count = 0L
    #bad_pic_count = 0L
    bad_pic_article_count = 0L
    lq_filter_shifted = 0L
    
    bad_non_cdn_oid = {}
    lq_filter_shifted_oid = []
    bad_size = []
    bad_bg = []
    bad_operate_tid = []
    bug_oid = []
    bad_pic_type = ['map', 'handwrite', 'banner', 'table', 'material', \
            'logo', 'plot', 'exp', 'word', 'code', 'MEDIOCRE']
    
    bad_pic, bad_pic_oid = {}, {}
    for item in bad_pic_type:
        
        bad_pic[item] = []
        bad_pic_oid[item] = []

    
    sim_pic = []
    sim_pic_oid = []

    l=len(os.listdir("./logs/"))
    SUMMARY=[0]*l
    FOUND=[0]*l
    CHANGED=[0]*l

    for filename in os.listdir(PATH):
        if filename.find('log')==-1:
            continue
       # mtime=datetime.fromtimestamp(os.stat(PATH+filename).st_mtime)
        print filename
        try:
            ind=filename.split('.')[-1]
            if ind=="log":
                ind=0
            else:
                ind = int(ind)
            summary,found,changed=get_summary_data(filename,sstime,setime)
            SUMMARY[ind]=summary
            FOUND[ind]=found
            CHANGED[ind]=changed
        except:
            print("%s file has error"% filename)
    
    SUMMARY_COUNT=sum(SUMMARY)
    found_count=sum(FOUND)
    changed_count=sum(CHANGED)
    
    print "total summary is {},total found and changed are ({},{}),respectivly".format(SUMMARY_COUNT,found_count,changed_count)
    db = SQL(sql_host, sql_port, sql_username, sql_password, sql_dbname)
    db.connectDB()
    
    sql = "select oid,pic,summary_info,tid,raw_msg,uptime from " + TABLE_NAME + \
            " where " + COLUMN_NAME + ">=%s and " + COLUMN_NAME + "<%s and type in (2, 3) "\
            " and nid not in %s order by uptime desc;"
    args1 = [stime,dtime,not_match_nids]
    datas = db.queryDB(sql,args1)
    
    sql = "select count(*) from " + TABLE_NAME + \
            " where " + COLUMN_NAME + ">=%s and " + COLUMN_NAME + "<%s and pic_editor=1 and type = 1 "\
            " and nid not in %s;"
    data_editor = db.queryDB(sql, args1)[0][0]
    
    sql = "select raw_msg from " + TABLE_NAME + \
            " where " + COLUMN_NAME + ">=%s and " + COLUMN_NAME + "<%s and pic_editor=1 and type = 1 "\
            " and nid not in %s;"
    raw_msgs =db.queryDB(sql, args1)
    pic_editor_count = 0L
    
    for item in raw_msgs:
        
        try:
            msg = json.loads(item[0])
            if 'pic_editor' in msg:
                pic_editor_count += len(msg['pic_editor'])
        except Exception as err:
            start = item[0].find("pic_editor")
            end = item[0].find("haveheadpic")
            temp = item[0][start-1:end-3]
            pic_editor_count += temp.count('.')

    for each in datas:
       
        total_count += 1
        oid = each[0]
        pics = each[1]
        summary = each[2]
        tid = each[3]
        raw_msg = each[4]
        uptime = datetime.strptime(str(each[5]), "%Y-%m-%d %H:%M:%S")
        if (down_error_stime-uptime).total_seconds() >= 60 * interval_min:
            temp_count2 = total_pic_body_count - temp_pic_count
            temp_count1 = bad_down_count - down_error_temp_count
            if temp_count2 > 0:
                temp =(temp_count1) / float(temp_count2)
                print str(down_error_stime),temp
                if temp >= down_error_max:
                    down_error_times.append((str(down_error_stime-dt.timedelta(minutes=interval_min)), \
                            str(down_error_stime),temp,temp_count1))
            down_error_stime = down_error_stime-dt.timedelta(minutes=interval_min)
            down_error_temp_count = bad_down_count
            temp_pic_count = total_pic_body_count

        try:
            raw_msg_d = json.loads(raw_msg)
            total_pic_body_count += len(raw_msg_d['pic_body'])
        except ValueError:
            if 'pic_body' in raw_msg:
                start = raw_msg.find('pic_body')
                total_pic_body_count += raw_msg[start:].count(',')
        try:
            raw_msg_d = json.loads(raw_msg)
            total_pic_full_count += len(raw_msg_d['pic_full'][0])
            if len(raw_msg_d['pic_full'][0]) != 0:
                article_has_pic_count += 1
        except Exception as err1:
            if 'pic_full' in raw_msg:
                start = raw_msg.find('pic_full')+12
                end = raw_msg.find('pic_body')-4
                total_pic_full_count += raw_msg[start:end].count("url_ori")
                if raw_msg[start:end].count("url_ori") != 0:
                    article_has_pic_count += 1
        try:
            pics = eval(pics)[0]
            if len(pics) != 0:
                total_pics_count += len(pics)
        except Exception:
            pass 
        try:
            data = json.loads(summary)
        except Exception:
            text = '<font color="#FF0000">Summary_info field truncated in DB: tid:%s</font>\n' % (tid)
            bad_operate_tid.append((str(tid),str(oid)))
            #print tid
            end = summary.find('pic_info')
            try:
                debug = json.loads(summary[0:end-3]+'}')['debug']
                if "error_func" in debug:
                    total_bug_number += debug["error_func"]
                    if debug["error_func"] > 0:
                        bug_oid.append(oid)
                    
                if "bad_upload" in debug:
                    bad_upload_img += debug["bad_upload"]
                    b = debug["bad_upload"]
                
                if "sent_kafka" in debug:
                    if debug["sent_kafka"] == True:
                        log_sent_kafka_count += 1
                    else:
                        log_not_sent_kafka_count += 1
                
                if "good_down_count" in debug:
                    good_down_count += debug["good_down_count"]
                
                if "bad_down_count" in debug:
                    bad_down_count += debug["bad_down_count"]

                    a = debug["bad_down_count"]
                
                if "bad_non_cdn_count" in debug:
                    bad_non_cdn_count += debug["bad_non_cdn_count"]
                    c = debug["bad_non_cdn_count"]
                
                total_network_number += (a+b+c)

                if "lq_filter_shifted" in debug:
                    if debug['lq_filter_shifted'] == True:
                        lq_filter_shifted += 1
                        lq_filter_shifted_oid.append(oid)
                continue
            except Exception:
                continue


        debug = data["debug"]
        
        if "error_func" in debug:
            total_bug_number += debug["error_func"]
            if debug["error_func"] > 0:
                bug_oid.append(oid)
        
        if "bad_upload" in debug:
            bad_upload_img += debug["bad_upload"]
            a = debug["bad_upload"]
        
        if "sent_kafka" in debug:
            if debug["sent_kafka"] == True:
                log_sent_kafka_count += 1
            else:
                log_not_sent_kafka_count += 1
        
        if "good_down_count" in debug:
            good_down_count += debug["good_down_count"]
        
        if "bad_down_count" in debug:
            bad_down_count += debug["bad_down_count"]
            b = debug["bad_down_count"]
        
        if "bad_non_cdn_count" in debug:
            bad_non_cdn_count += debug["bad_non_cdn_count"]
            c = debug["bad_non_cdn_count"]
        total_network_number += (a+b+c)
        
        if "lq_filter_shifted" in debug:
            if debug['lq_filter_shifted'] == True:
                lq_filter_shifted += 1
                lq_filter_shifted_oid.append(oid)
                if len(data["pic_info"]) == 2 and "GOOD" not in data["pic_info"][0]['(324, 216)']["labels"]:
                    #print oid
                    continue

        pic_info = data["pic_info"]
        
        for pic in pic_info:
            
            for key in pic:
                
                if key == '(690, 340)':
                    continue
                info = pic[key]
                lables = info["labels"]
                if len(lables) != 0:

                    lables = filter(lambda x : x != None, lables)
                    
                    if "BAD_CDN" in lables:
                        url = {}
                        ori = info["url_ori"]
                        url['url_ori'] = '<a href="%s">%s</a>' % (ori, ori)
                        crop = info["url_crop"]
                        url['url_crop'] = '<a href="%s">%s</a>' % (crop, crop)
                        if oid not in bad_non_cdn_oid:
                            bad_non_cdn_oid[oid] = 'CDN_BAD:oid=%s,url=%s<p>\n' % (oid, url)
                    
                    if "BACKGROUND_BAD" in lables:
                        bad_bg_img += 1
                        url = {}
                        ori = info["url_ori"]
                        url['url_ori'] = '<a href="%s">%s</a>' % (ori, ori)
                        crop = info["url_crop"]
                        url['url_crop'] = '<a href="%s">%s</a>' % (crop, crop)
                        bad_bg.append('BACKGROUND_BAD:oid=%s,url=%s<p>\n' % (oid, url))
                    
                    if "BAD_SIZE" in lables:
                        bad_size_count += 1
                        url = {}
                        ori = info["url_ori"]
                        url['url_ori'] = '<a href="%s">%s</a>' % (ori, ori)
                        crop = info["url_crop"]
                        url['url_crop'] = '<a href="%s">%s</a>' % (crop, crop)
                        bad_size.append('SIZE_BAD:oid=%s,url=%s<p>\n' % (oid, url))
                    
                    if len(list(set(bad_pic_type) & set(lables))) > 0:
                        #bad_pic_count += 1
                        url = {}
                        ori = info["url_ori"]
                        url['url_ori'] = '<a href="%s">%s</a>' % (ori, ori)
                        crop = info["url_crop"]
                        url['url_crop'] = '<a href="%s">%s</a>' % (crop, crop)
                        pic_types = list(set(bad_pic_type) & set(lables))
                        for pic_type in pic_types:
                            
                            bad_pic[pic_type].append('%s:oid=%s,url=%s<p>\n' % \
                                    (pic_type.upper(), oid, url))
                            if oid not in bad_pic_oid[pic_type]:
                                bad_pic_oid[pic_type].append(oid)
                            #else:
                            #    if pic_type == 'MEDIOCRE':
                            #        print oid
                    
                    if "similar" in map(lambda x:x[:7], lables):
                        #sim_pic_count += 1
                        url = {}
                        ori = info["url_ori"]
                        url['url_ori'] = '<a href="%s">%s</a>' % (ori, ori)
                        crop = filter(lambda x:len(x) > 11, lables)[0][11:] 
                        url['url_similar'] = '<a href="%s">%s</a>' % (crop, crop)
                        sim_pic.append('SIMILAR_PIC:oid=%s,url=%s<p>\n' % (oid, url))
                        if oid not in sim_pic_oid:
                            sim_pic_oid.append(oid)
                            sim_pic_article_count += 1
    bad_art_oids = []
    for item in bad_pic_oid:
        if item in ['banner', 'map']:
            continue
        for oid in bad_pic_oid[item]:
            
            if oid not in bad_art_oids:
                bad_art_oids.append(oid)

    if total_count == 0:
        log_not_sent_kafka_count_f = r'n/a'
        log_sent_kafka_count_f = r'n/a'
    else:
        log_not_sent_kafka_count_f = '%.4f%%' % (log_not_sent_kafka_count/float(total_count)*100)
        log_sent_kafka_count_f = '%.4f%%' % (log_sent_kafka_count/float(total_count)*100)
    
    if total_pics_count == 0:
        total_network_number_f1 = r'n/a'
        bad_non_cdn_count_f1 = r'n/a'
        good_down_count_f1 = r'n/a'
        bad_down_count_f1 = r'n/a'
        bad_bg_img_f1 = r'n/a'
        bad_size_count_f1 =r'n/a'
        bad_upload_img_f1=r'n/a'
        map_pic_f = r'n/a' 
        handwrite_pic_f = r'n/a' 
        banner_pic_f = r'n/a' 
        table_pic_f = r'n/a' 
        material_pic_f = r'n/a' 
        logo_pic_f = r'n/a' 
        plot_pic_f = r'n/a' 
        exp_pic_f = r'n/a' 
        word_pic_f = r'n/a' 
        code_pic_f = r'n/a' 
        bad_pic_article_f = r'n/a'
        sim_pic_article_f = r'n/a'
        MEDIOCRE_f = r'n/a'
    else:
        total_network_number_f1 = '%.4f%%' % (total_network_number/float(total_pic_full_count)*100)
        bad_non_cdn_count_f1 = '%.4f%%' % (bad_non_cdn_count/float(total_pic_full_count)*100)
        good_down_count_f1 = '%.4f%%' % (good_down_count/float(total_pic_full_count)*100)
        bad_down_count_f1 = '%.4f%%' % (bad_down_count/float(total_pic_full_count)*100)
        bad_bg_img_f1 = '%.4f%%' % (bad_bg_img/float(total_pic_full_count)*100)
        bad_size_count_f1 = '%.4f%%' % (bad_size_count/float(total_pic_full_count)*100)
        bad_upload_img_f1 = '%.4f%%' % (bad_upload_img/float(total_pic_full_count)*100)
        map_pic_f = '%.4f%%' % (len(bad_pic_oid['map'])/float(total_pic_full_count)*100) 
        handwrite_pic_f = '%.4f%%' % (len(bad_pic_oid['handwrite'])/float(total_pic_full_count)*100) 
        banner_pic_f = '%.4f%%' % (len(bad_pic_oid['banner'])/float(total_pic_full_count)*100)
        table_pic_f = '%.4f%%' % (len(bad_pic_oid['table'])/float(total_pic_full_count)*100)
        material_pic_f = '%.4f%%' % (len(bad_pic_oid['material'])/float(total_pic_full_count)*100)
        logo_pic_f = '%.4f%%' % (len(bad_pic_oid['logo'])/float(total_pic_full_count)*100)
        plot_pic_f = '%.4f%%' % (len(bad_pic_oid['plot'])/float(total_pic_full_count)*100)
        exp_pic_f = '%.4f%%' % (len(bad_pic_oid['exp'])/float(total_pic_full_count)*100)
        word_pic_f = '%.4f%%' % (len(bad_pic_oid['word'])/float(total_pic_full_count)*100)
        code_pic_f = '%.4f%%' % (len(bad_pic_oid['code'])/float(total_pic_full_count)*100)
        bad_pic_article_f = '%.4f%%' % (len(bad_art_oids)/float(total_pic_full_count)*100) 
        sim_pic_article_f = '%.4f%%' % (sim_pic_article_count/float(total_pic_full_count)*100)
        MEDIOCRE_f = '%.4f%%' % (len(bad_pic_oid['MEDIOCRE'])/float(total_pic_full_count)*100)

    if article_has_pic_count == 0:
        total_network_number_f = r'n/a'
        bad_non_cdn_count_f = r'n/a'
        good_down_count_f = r'n/a'
        bad_down_count_f = r'n/a'
        bad_upload_img_f = r'n/a'
        bad_bg_img_f = r'n/a'
        bad_size_count_f = r'n/a'
        pics_body_article = r'n/a'
        pics_head_article = r'n/a'
        pics_body_no_editor_article = r'n/a'
        pics_head_no_editor_article = r'n/a'
        bad_pic_article = r'n/a'
        sim_pic_article = r'n/a'
        map_pic = r'n/a' 
        handwrite_pic = r'n/a' 
        banner_pic = r'n/a' 
        table_pic = r'n/a' 
        material_pic = r'n/a' 
        logo_pic = r'n/a' 
        plot_pic = r'n/a' 
        exp_pic = r'n/a' 
        word_pic = r'n/a' 
        code_pic = r'n/a'
        MEDIOCRE = r'n/a'
    else:
        total_network_number_f = '%.4f%%' % (total_network_number/float(article_has_pic_count)*100)
        bad_non_cdn_count_f = '%.4f%%' % (bad_non_cdn_count/float(article_has_pic_count)*100)
        good_down_count_f = '%.4f%%' % (good_down_count/float(article_has_pic_count)*100)
        bad_down_count_f = '%.4f%%' % (bad_down_count/float(article_has_pic_count)*100)
        bad_upload_img_f = '%.4f%%' % (bad_upload_img/float(article_has_pic_count)*100)
        bad_bg_img_f = '%.4f%%' % (bad_bg_img/float(article_has_pic_count)*100)
        bad_size_count_f = '%.4f%%' % (bad_size_count/float(article_has_pic_count)*100)
        pics_body_article = '%.4f' % (total_pic_body_count/float(article_has_pic_count))
        pics_head_article = '%.4f' % (total_pics_count/float(article_has_pic_count))
        pics_body_no_editor_article = '%.4f' % ((total_pic_body_count-pic_editor_count)/\
                float(article_has_pic_count-data_editor))
        pics_head_no_editor_article = '%.4f' % ((total_pics_count-pic_editor_count)/\
                float(article_has_pic_count-data_editor))
        bad_pic_article = '%.4f%%' % (len(bad_art_oids)/float(article_has_pic_count)*100)
        sim_pic_article = '%.4f%%' % (sim_pic_article_count/float(article_has_pic_count)*100)
        map_pic = '%.4f%%' % (len(bad_pic['map'])/float(article_has_pic_count)*100) 
        handwrite_pic = '%.4f%%' % (len(bad_pic['handwrite'])/float(article_has_pic_count)*100) 
        banner_pic = '%.4f%%' % (len(bad_pic['banner'])/float(article_has_pic_count)*100)
        table_pic = '%.4f%%' % (len(bad_pic['table'])/float(article_has_pic_count)*100)
        material_pic = '%.4f%%' % (len(bad_pic['material'])/float(article_has_pic_count)*100)
        logo_pic = '%.4f%%' % (len(bad_pic['logo'])/float(article_has_pic_count)*100)
        plot_pic = '%.4f%%' % (len(bad_pic['plot'])/float(article_has_pic_count)*100)
        exp_pic = '%.4f%%' % (len(bad_pic['exp'])/float(article_has_pic_count)*100)
        word_pic = '%.4f%%' % (len(bad_pic['word'])/float(article_has_pic_count)*100)
        code_pic = '%.4f%%' % (len(bad_pic['code'])/float(article_has_pic_count)*100)
        MEDIOCRE = '%.4f%%' % (len(bad_pic['MEDIOCRE'])/float(article_has_pic_count)*100)
    
    bug_oid=bug_oid[:30]
    
    message='''%s-%s:<br/>
    total_articles_count:%d<br/>
    article_with_pic_count:%d<br/>
    article_with_pic_editor_count:%d<br/>
    article_with_non_cdn_count:%d<br/>
    total_pic_editor_count:%d<br/>
    total_head_pics_count:%d<br/>
    total_pics_body_count:%d<br/>
    total_pics_processed_count:%d<br/>
    lq_filter_shifted:%d<br/>
    

    图片总数/有图文章数:%s<br/>
    头图总数/有图文章数:%s<br/>
    非编辑指定图总数/非编辑指定文章数:%s<br/>
    非编辑指定头图数/非编辑指定文章数:%s<br/>

    频道改图程序处理文章数:%d<br/>
    检查频道内文章数:%d<br/>
    频道内更改头图文章数:%d<br/>
   
    
    
    <table border='1' style="width:800px">
    <tr><th></th><th>Count</th><th>Percent Over Total Job</th></tr>
    <tr><td>log_not_sent_kafka_count</td><td>%d</td><td><font color="#FF0000">%s</font></td></tr>
    <tr><td>log_sent_kafka_count</td><td>%d</td><td>%s</td></tr>
    </table>
    <br/>
    <table border='1' style="width:800px">
    <tr><th></th><th>Count</th><th>Count Over Total Processed Pics</th><th>Count Over Total Articles With Pics</th></tr>
    <tr><td>total_network_number</td><td>%d</td><td>%s</td><td>%s</td></tr>
    <tr bgcolor=#ADADAD><td>bad_non_cdn_pic_count</td><td>%d</td><td>%s</td><td>%s</td></tr>
    <tr bgcolor=#ADADAD><td>good_down_count</td><td>%d</td><td>%s</td><td>%s</td></tr>
    <tr bgcolor=#ADADAD><td>bad_down_count</td><td>%d</td><td>%s</td><td>%s</td></tr>
    <tr><td>bad_upload_img</td><td>%d</td><td>%s</td><td>%s</td></tr>
    <tr><td>bad_bg_img</td><td>%d</td><td>%s</td><td>%s</td></tr>
    <tr><td>bad_size_count</td><td>%d</td><td>%s</td><td>%s</td></tr>
    <tr><td>article_with_similar_pic</td><td><font color="#3300FF">%d</font></td><td><font color="#3300FF">%s</font></td><td><font color="#3300FF">%s</font></td></tr>
    <tr><td>article_with_low_pic</td><td><font color="#3300FF">%d</font></td><td><font color="#3300FF">%s</font></td><td><font color="#3300FF">%s</font></td></tr>
    </table>
    <br/>
    <table border='1' style="width:800px">
    <tr><th></th><th>Pic Count</th><th>Article Count</th><th>Pic Count Over Total Processed Pics</th>\
            <th>Article Count Over Total Articles With Pics</th></tr>
    <tr bgcolor=#ADADAD><td>MEDIOCRE</td><td>%d</td><td>%d</td><td>%s</td><td>%s</td></tr>
    <tr bgcolor=#ADADAD><td>map_pic</td><td><font color="#3300FF">%d</font></td>\
            <td><font color="#3300FF">%d</font></td>\
            <td><font color="#3300FF">%s</font></td>\
            <td><font color="#3300FF">%s</font></td></tr>
    <tr bgcolor=#ADADAD><td>handwrite_pic</td><td><font color="#9400D3">%d</font></td>\
            <td><font color="#9400D3">%d</font></td>\
            <td><font color="#9400D3">%s</font></td>\
            <td><font color="#9400D3">%s</font></td></tr>
    <tr bgcolor=#ADADAD><td>banner_pic</td><td><font color="#3300FF">%d</font></td>\
            <td><font color="#3300FF">%d</font></td>\
            <td><font color="#3300FF">%s</font></td>\
            <td><font color="#3300FF">%s</font></td></tr>
    <tr bgcolor=#ADADAD><td>table_pic</td><td>%d</td><td>%d</td><td>%s</td><td>%s</td></tr>
    <tr bgcolor=#ADADAD><td>material_pic</td><td>%d</td><td>%d</td><td>%s</td><td>%s</td></tr>
    <tr bgcolor=#ADADAD><td>logo_pic</td><td><font color="#9400D3">%d</font></td>\
            <td><font color="#9400D3">%d</font></td>\
            <td><font color="#9400D3">%s</font></td>\
            <td><font color="#9400D3">%s</font></td></tr>
    <tr bgcolor=#ADADAD><td>plot_pic</td><td>%d</td><td>%d</td><td>%s</td><td>%s</td></tr>
    <tr bgcolor=#ADADAD><td>exp_pic</td><td>%d</td><td>%d</td><td>%s</td><td>%s</td></tr>
    <tr bgcolor=#ADADAD><td>word_pic</td><td>%d</td><td>%d</td><td>%s</td><td>%s</td></tr>
    <tr bgcolor=#ADADAD><td>code_pic</td><td>%d</td><td>%d</td><td>%s</td><td>%s</td></tr>
    </table>
    <font color="#FF0000">BUG_NUMBER:%d  oid:%s</font><br/>
    '''%(\
        stime,dtime,\
        total_count,\
        article_has_pic_count,\
        data_editor,\
        len(bad_non_cdn_oid.keys()),\
        pic_editor_count,\
        total_pics_count,\
        total_pic_body_count,\
        total_pic_full_count,\
        lq_filter_shifted,\

        pics_body_article,\
        pics_head_article,\
        pics_body_no_editor_article,\
        pics_head_no_editor_article,\


        SUMMARY_COUNT,\
        found_count,\
        changed_count,\

        
        log_not_sent_kafka_count,log_not_sent_kafka_count_f,\
        log_sent_kafka_count,log_sent_kafka_count_f,\
        total_network_number,total_network_number_f1,total_network_number_f,\
        bad_non_cdn_count,bad_non_cdn_count_f1,bad_non_cdn_count_f,\
        good_down_count,good_down_count_f1,good_down_count_f,\
        bad_down_count,bad_down_count_f1,bad_down_count_f,\
        bad_upload_img,bad_upload_img_f1,bad_upload_img_f,\
        bad_bg_img,bad_bg_img_f1,bad_bg_img_f,\
        bad_size_count,bad_size_count_f1,bad_size_count_f,\
        
        sim_pic_article_count,sim_pic_article_f,sim_pic_article,\
        len(bad_art_oids),bad_pic_article_f,bad_pic_article,\
        len(bad_pic[bad_pic_type[10]]),len(bad_pic_oid[bad_pic_type[10]]),MEDIOCRE_f,MEDIOCRE,\
        len(bad_pic[bad_pic_type[0]]),len(bad_pic_oid[bad_pic_type[0]]),map_pic_f,map_pic,\
        len(bad_pic[bad_pic_type[1]]),len(bad_pic_oid[bad_pic_type[1]]),handwrite_pic_f,handwrite_pic,\
        len(bad_pic[bad_pic_type[2]]),len(bad_pic_oid[bad_pic_type[2]]),banner_pic_f,banner_pic,\
        len(bad_pic[bad_pic_type[3]]),len(bad_pic_oid[bad_pic_type[3]]),table_pic_f,table_pic,\
        len(bad_pic[bad_pic_type[4]]),len(bad_pic_oid[bad_pic_type[4]]),material_pic_f,material_pic,\
        len(bad_pic[bad_pic_type[5]]),len(bad_pic_oid[bad_pic_type[5]]),logo_pic_f,logo_pic,\
        len(bad_pic[bad_pic_type[6]]),len(bad_pic_oid[bad_pic_type[6]]),plot_pic_f,plot_pic,\
        len(bad_pic[bad_pic_type[7]]),len(bad_pic_oid[bad_pic_type[7]]),exp_pic_f,exp_pic,\
        len(bad_pic[bad_pic_type[8]]),len(bad_pic_oid[bad_pic_type[8]]),word_pic_f,word_pic,\
        len(bad_pic[bad_pic_type[9]]),len(bad_pic_oid[bad_pic_type[9]]),code_pic_f,code_pic,\
        total_bug_number,json.dumps(bug_oid))
    
    #print message


    with open('log.txt','w') as fp:
        
        fp.write(message)
        text = '<font color="#FF0000">Summary_info field truncated in DB: (tid,oid):%s</font>\n'\
                % (bad_operate_tid)
        fp.write(text)
        fp.write('<br/>')
       
        for item in down_error_times:
            print item
            fp.write('bad down time: {}'.format(item))
            fp.write('<br/>')
       
        log_total_count = []
        res_oids = []
        with open(LOG_PATH + 'out_13.txt') as f:
            
            lines = f.readlines()
            line1 = eval(lines[0].rstrip('\r\n'))
            line3 = filter(lambda x : x != '', lines[2].rstrip('\r\n').split(','))
            if line1['stime'] == str(stime):
                log_total_count.append(int(line1['count']))
                res_oids = [0,[]] if len(line3) == 1 else [int(line3[0]),line3[1:]]

            else:
                log_total_count.append(0)
                res_oids = [0,[]]
        with open(LOG_PATH + 'out.txt') as f:
            
            lines = f.readlines()
            line1 = eval(lines[0].rstrip('\r\n'))
            line3 = filter(lambda x : x != '', lines[2].rstrip('\r\n').split(','))
            if line1['stime'] == str(stime):
                log_total_count.append(int(line1['count']))
                if len(line3) > 1:
                    res_oids[0] += int(line3[0])
                    res_oids[1].extend(line3[1:])

            else:
                log_total_count.append(0)
        
        
        fp.write('日志统计oid次数：{} + {} = {} '.\
                format(log_total_count[0], log_total_count[1], sum(log_total_count)))
        
        if total_count != 0:
            temp = float(sum(log_total_count)) / total_count * 100
            if temp  >= log_count_max:
                fp.write('<font color="#FF0000">({})</font>'.format('%.4f%%'%(temp)))
            else:
                fp.write('<font>({})</font>'.format('%.4f%%'%(temp)))
        else:
            fp.write('<font>(n/a)</font>')
        fp.write('<br/>')
        fp.write('oid收到超过5次的个数及样例：{} : {}'.format(res_oids[0], res_oids[1]))
        fp.write('<br/>')


        i = 0
        for oid in bad_non_cdn_oid:
            i += 1
            fp.write(bad_non_cdn_oid[oid])
            if i == CASE_COUNT:
                break
            
        for item in bad_pic_type:
            pics = bad_pic[item][:CASE_COUNT]
            random.shuffle(pics)
            for pic in pics:
                fp.write(pic)
       
        sim_pic = sim_pic[:CASE_COUNT]
        for item in sim_pic:
            fp.write(item)
        
        bad_bg = bad_bg[:CASE_COUNT]
        for item in bad_bg:
            
            fp.write(item)
        
        bad_size = bad_size[:CASE_COUNT]
        for item in bad_size:
            fp.write(item) 
            
        
        fp.close()
    
    sendEmail(TABLE_NAME, COLUMN_NAME, dev)

    exit(0)







    
    

