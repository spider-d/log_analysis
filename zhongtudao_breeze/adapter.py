#! /usr/bin/env python
# -*- coding: utf-8 -*-
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
r"""Breeze adapter module for XXXX.

Service: http://wiki.babel.baidu.com/twiki/bin/view/Com/Test/Breeze

Update: 2013-02-27 17:40:18
        update logic layer stage-1 by lilei05@

Update: 2013-02-27 17:40:18
Author: QAPS-WSP-Antispam
Copyright (c) 2013 Baidu.com, Inc. All Rights Reserved.
"""
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# configuration

import os
import sys
import re
import json
import time
import shelve
import urllib
import subprocess
DIR_THIS = os.path.dirname(os.path.abspath(__file__))
DIR_HOME = os.path.dirname(DIR_THIS)
sys.path.append(DIR_HOME)

# current adapter server port, starts from 8601
PORT = 8600

# python encoding for arguments and returns
ENCODING_PY = 'utf8'

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# API functions

real_path = os.path.realpath(__file__)
current_dir = os.path.dirname(real_path)
sys.path.append(os.path.join(current_dir,'app/scholar_url'))
from data_process import *
from mongo_operate import *
from make_xml import *



#学术数据流查询
def getDataFlow(url):
    sys.path.append(os.path.join(current_dir,'app/data_flow'))
    from flow_main import *
    pattern = re.compile(r'((http|ftp|https)://)(([a-zA-Z0-9\._-]+\.[a-zA-Z]{2,6})|([0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}))(:[0-9]{1,4})*(/[a-zA-Z0-9\&%_\./-~-]*)?')
    return_data = {}
    url = urllib.unquote(url)
    if not url or (len(url)>=2000) or not pattern.match(url):
        return_data['retcode'] = -1
        return_data['data'] = {u'查询错误':u'url非法'}
        return return_data
    se_result = main(url)
    return_data['retcode'] = 0
    return_data['data'] = se_result
    return return_data



#调用pdfcase自动化分析服务
def runPdfAutoCase(date_time):
    return_data = {}
    log_path = os.path.join(current_dir,'log/pdf_autocase.log')
    f_log = open(log_path,'w')
    try:
        os.system('rm -rf %s' % os.path.join(current_dir,'log/*.log.*'))
        cmd = 'python %s %s' % ('/home/offline_data/pdfurl_autoanalyse/main_run.py',date_time)
        p = subprocess.Popen(cmd,shell=True,stdout=f_log.fileno(),stderr=f_log.fileno())
        pid = p.pid
        return_data['status'] = 0
        return_data['pid'] = pid
    except Exception,e:
        return_data['data'] = e
    return return_data

#返回pdfcase的生成日期
def getPdfCaseDate():
    return_data = {}
    try:
        client = connect('127.0.0.1',27017)
        db = client.scholar
    except:
        return_data['retcode'] = -1
        return_data['data'] = u'数据库连接失败'
        return return_data
    date_list = db.pdf_autocase.distinct('date_time')
    if not date_list:
        return_data['retcode'] = 1
        return_data['data'] = u'没有分析数据'
        return return_data
    return_data['retcode'] = 0
    return_data['data'] = date_list
    return return_data

#查询pdfcase分析结果
def getPdfCaseData(date,lang):
    return_data = {}
    try:
        client = connect('127.0.0.1',27017)
        db = client.scholar
    except:
        return_data['retcode'] = -1
        return_data['data'] = u'数据库连接失败'
        return return_data
    data = db.pdf_autocase.find({'date_time':date,'language_type':lang},{'_id':0}).sort("case_type")
    if not data:
        return_data['retcode'] = 1
        return_data['data'] = u'查询结果为空'
        return return_data
    data_list = []
    for e in data:
        data_list.append(e)
    return_data['retcode'] = 0
    return_data['data'] = data_list
    return return_data

#中途岛feed日志查询
def getFeedData(date_time, data_type):
    return_data = {}
    try:
        client = connect('127.0.0.1',27017)
        db = client['zhongtudao']
    except:
        return_data['status'] = -1
        return_data['data'] = u'数据库连接失败'
        return return_data
    if data_type not in ['pos_click','from_click','type_click','provider_click','time_click','time_show','image_num']:
        return_data['status'] = -2
        return_data['data'] = u'查询类型错误'
        return return_data
    con_list = data_type.split('_')
    if con_list[1]!='click':
        data = db['feed_result'].find({'datetime':{'$regex':'%s\d*'%date_time}},{data_type:1,'_id':0,'total_count':1,'total_click_count':1,'datetime':1}).sort('datetime')
    else:
        con_str = con_list[0]+'_'+'show'
        data = db['feed_result'].find({'datetime':{'$regex':'%s\d*'%date_time}},{data_type:1,con_str:1,'_id':0,'total_count':1,'total_click_count':1,'datetime':1}).sort('datetime')
    if not data:
        return_data['retcode'] = 1
        return_data['data'] = u'数据为空'
        return return_data
    data_list = []
    for e in data:
        data_list.append(e)
    if not data_list:
        return_data['retcode'] = 1
        return_data['data'] = u'数据为空'
        return return_data
    return_data['retcode'] = 0
    return_data['data'] = data_list
    return return_data

def get_ztd_data(log_type, result_type, date_time):
    sys.path.append(os.path.join(current_dir,'app/zhongtudao'))
    from get_data import *
    return_data = {}
    if log_type == 'tc_webb':
        return get_tc_webb(result_type, date_time)
    if log_type == 'feed':
        return get_feed(result_type, date_time)
    return_data['retcode'] = 1
    return return_data

#根据日期查询随机url点展比
def get_random_url_data(date_time):   
    return_data = {}
    try:
        client = connect('127.0.0.1',27017)
        db = client['zhongtudao']
        search_col = db['random_url']
    except:
        return_data['status'] = -1
        return_data['data'] = u'数据库连接失败'
        return return_data
    item_list = ['datetime', 'url', 'click_count', 'show_count', 'click_show_rate']
    search_result = search_col.find({'datetime':date_time})
    for data_item in search_result:
        date_data_list = []
        for item in item_list:
            date_data_list.append(data_item.get(item,''))
        data_list.append(date_data_list)
    return_data['data_list'] = data_list
    return_data['retcode'] = 0
    client.close()
    return return_data


        


#根据学术url查询xml数据
def seScholarXml(scholarurl):
    query_url = scholarurl
    if not query_url:
        return {'retcode':1,'data':u'查询条件不能为空'}
    client = connect('127.0.0.1',27017)
    db = client.scholar
    scholar_data = db.scholar_xml.find_one({'scholar_url':query_url})
    if scholar_data:
        scholar_xml = scholar_data.get('scholar_xml','')
        if scholar_xml:
            return {'retcode':0,'data':scholar_xml}
    return {'retcode':-1,'data':u'无对应数据'}
#学术论文时效性添加
def addScholarUrl(data):
    json_data = data_check(data)
    if json_data.get('retcode',''):
        return json_data
    filtered_data = data_filter(json_data)
    scholarurl = filtered_data.get('scholarurl')
    xml_format = scholar_xml(filtered_data).strip().replace('<?xml version="1.0" ?>','<?xml version="1.0" encoding="UTF-8"?>')
    #repl = lambda x: ">%s<" % x.group(1).strip() if len(x.group(1).strip()) != 0 else x.group(0)
    xml_data = re.sub(r'>\n\s*<',"><",xml_format)
    timestamp = int(time.time())
    data_dict = {'scholar_url':scholarurl,'scholar_xml':xml_data,'timestamp':timestamp,'status':0}
    client = connect('127.0.0.1',27017)
    db = client.scholar
    db.scholar_xml.update({'scholar_url':scholarurl},{'$set':data_dict},True,True)
    client.close()
    return {'retcode':0,'data':xml_format}

#post测试接口
def test_post(data):
    return_data = {}
    if not isinstance(data,dict):
        return_data['status'] = -1
        return_data['data'] = u'数据格式非法!'
        return return_data
    username = data.get('name','')
    password = data.get('password','')
    if username and password:
        return_data['status'] = 0
        return_data['data'] = u'登陆成功'
        return return_data
    return_data['status'] = 1
    return_data['data'] = u'用户名/密码不能为空!'
    return return_data



#appsearch数据查询
app_summary_dir = os.path.join(current_dir,'../appsearch/datalog/data/app_summary')
ubs_summary_dir = os.path.join(current_dir,'../appsearch/datalog/data/ubs_log/summary_log')
def getTuneUpData(appName,queryDate):
    app_name = appName
    date = queryDate
    result = {}
    app_summary_path = os.path.join(app_summary_dir,app_name)
    if not os.path.exists(app_summary_path):
        result['retcode'] = -1
        result['data'] = 'no this app'
        return result
    db = shelve.open(app_summary_path)
    data = db.get(date,'')
    if data:
        result['retcode'] = 0
        result['data'] = data
        db.close()
        return result
    else:
        result['retcode'] = 1
        result['data'] = 'no data'
        db.close()
        return result

def getUbsData(appName,queryDate):
    app_name = appName
    date = queryDate
    result = {}
    ubs_summary_path = os.path.join(ubs_summary_dir,app_name)
    if not os.path.exists(ubs_summary_path):
        result['retcode'] = -1
        result['data'] = 'no this app'
        return result
    db = shelve.open(ubs_summary_path)
    data = db.get(date,'')
    if data:
        result['retcode'] = 0
        result['data'] = data
        db.close()
        return result
    else:
        result['retcode'] = 1
        result['data'] = 'no data'
        db.close()
        return result


'''
def getFilesListByShell(sPath):
    r"""调用 /bin/ls，返回指定目录下的文件列表。
    Return Detail:
        {
            "data" : 正常时返回文件列表，异常时返回 null
        }
    Parameters Detail:
        {
            "sPath" : 路径字符串
        }
    Parameters Example:
        {
            "sPath" : "/home/work/"
        }
    """
    import qa_subprocess
    dResult = qa_subprocess.callProc(lCmd=['/bin/ls','-A',sPath])
    dResult['data'] = dResult['stdout'].split() if dResult['ret'] == 0 else None
    return dResult
    
def getFilesListByPython(sPath):
    r"""调用 python，返回指定目录下的文件列表。
    Return Detail:
        {
            "data" : 正常时返回文件列表，异常时返回 null
        }
    Parameters Detail:
        {
            "sPath" : 路径字符串
        }
    Parameters Example:
        {
            "sPath" : "/home/work/"
        }
    """
    try:
        lFiles = os.listdir(sPath)
    except OSError as e:
        lFiles = None
    finally:
        return {'data':lFiles}
''' 
