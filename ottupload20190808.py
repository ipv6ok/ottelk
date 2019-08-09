# -*- coding: utf-8 -*-
# write by momeicai czm
# version:v1.0
# update in:2019-8-9
#####################
from __future__ import division
# import requests
import json
import sys
import logging
from elasticsearch import Elasticsearch
from elasticsearch import helpers
import time
from datetime import datetime, timedelta, tzinfo
import pytz
from tzlocal import get_localzone

import threading

#####################
reload(sys)
sys.setdefaultencoding('utf8')

# for logger setting
logger = logging.getLogger(__name__)
logger.setLevel(level=logging.INFO)
handler = logging.FileHandler("dumpott2jt.log")
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

logger.info("Start print log")

warntime = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
tz = get_localzone()  # 获得本地timezone
utc = pytz.utc  # 获得UTC timezone
now = datetime.today()
loc_dt = tz.localize(now)
utc_dt = loc_dt.astimezone(utc)
today = datetime.strftime(utc_dt, '%Y.%m.%d')

####kibana显示是需要时间区信息所以需要添加
#####################
####kibana显示是需要时间区信息所以需要添加
ZERO_TIME_DELTA = timedelta(0)
LOCAL_TIME_DELTA = timedelta(hours=8)


class LocalTimezone(tzinfo):
    def utcoffset(self, dt):
        return LOCAL_TIME_DELTA

    def dst(self, dt):
        return ZERO_TIME_DELTA

    def tzname(self, dt):
        return '+08:00'

#####################
def update_dir_key(mydir, keyip, keyname, keyvalue):
    if keyip in mydir:
        if keyname in mydir[keyip]:
            pass
            #print("keyname is %s" % keyname)
        else:
            mydir[keyip][keyname] = keyvalue
    else:
        mydir[keyip] = {keyname: keyvalue}
    #return mydir

#####################
def get_dir_key(mydir, keyip, keyname, keyvalue):
    if keyname in mydir[keyip]:
        return mydir[keyip][keyname]
    else:
        return 0
    #return mydir
#####################
# 质差ts(服务时长>10s)数量
def get_ts_stat(index_today, es, manu, mydir):
    # hy is s,huawei and zte is ms
    tsflag = 10000
    if (manu == "杭研"):
        tsflag = 10
    elif (manu == "华为"):
        tsflag = 10000
    elif (manu == "中兴"):
        # 中兴质差，通过用户日志无法计算，跳过
        return mydir

    querybody = {
        "size": 0,
        "query": {
            "bool": {
                "must": [{
                    "range": {
                        "@timestamp": {
                            "gt": "now-5m",
                            "lt": "now"
                        }
                    }
                },
                    {
                        "term": {
                            "viewtype.keyword": "直播"
                        }
                    }
                ]
            }
        },
        "aggs": {
            "citytype": {
                "terms": {"field": "city.keyword"},
                "aggs": {
                    "iptype": {
                        "terms": {"field": "host.name.keyword","size":2000},
                        "aggs": {
                            "costtime_range": {
                                "range": {
                                    "ranges": [
                                        {
                                            "from": tsflag,
                                            "key": "ts_bad"
                                        },
                                        {
                                            "to": tsflag,
                                            "key": "ts_good"
                                        }
                                    ],
                                    "field": "costtime"
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    response_byte = es.search(index=index_today, body=querybody)
    if (response_byte['hits']['total'] < 1):
        logger.info("get_ts_stat hits total less to 1")
        logger.info(querybody)
        logger.info(response_byte)
        return mydir


    for bucket in response_byte['aggregations']['citytype']['buckets']:
        cityname = bucket['key']
        for b in bucket['iptype']['buckets']:
            ipname = b['key']
            ts_count = b['doc_count']
            ts_bad = 0
            for s in b['costtime_range']['buckets']:
                keyname = s['key']
                # print("keyname is %s" % keyname)
                if (keyname == 'ts_bad'):
                    ts_bad = s['doc_count']
            #print("ipname is %s,ts_count is %s,ts_bad is %s" % (ipname,ts_count,ts_bad))
            update_dir_key(mydir, ipname, "city", cityname)
            update_dir_key(mydir, ipname, "tscount", ts_count)
            update_dir_key(mydir, ipname, "tsbadcount", ts_bad)

    return mydir


######################################
# 只统计广西合法clientip地址，非广西地址另行分析
# 返回码失败（4**，5**）数量
def get_httpstatus_stat(index_today, es, manu, mydir):
    querybody = {
        "size": 0,
        "query": {
            "bool": {
                "must": [
                    {
                        "range": {
                            "@timestamp": {
                                "gt": "now-5m",
                                "lt": "now"
                            }
                        }
                    },
                    {
                        "term": {
                            "viewtype.keyword": "直播"
                        }
                    }
                ]
            }
        },
        "aggs": {
            "citystat": {
                "terms": {
                    "field": "city.keyword"
                },
                "aggs": {
                    "clientipstat": {
                        "terms": {
                            "field": "host.name.keyword"
                            , "size": 2000
                        },
                        "aggs": {
                            "httpstatusstat": {
                                "terms": {
                                    "field": "httpstatus.keyword"
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    response_byte = es.search(index=index_today, body=querybody)
    if (response_byte['hits']['total'] < 1):
        logger.info("get_httpstatus_stat hits total less to 1")
        logger.info(querybody)
        logger.info(response_byte)
        return mydir

    for bucket in response_byte['aggregations']['citystat']['buckets']:
        cityname = bucket['key']
        for bb in bucket['clientipstat']["buckets"]:
            ipname = bb['key']
            doc_sum = bb['doc_count']
            start45_count = 0
            for s in bb['httpstatusstat']['buckets']:
                keyname = s['key']
                if keyname.startswith('4') or keyname.startswith('5'):
                    start45_count += s['doc_count']

            #print("ipname is %s,start45_count is %s" % (ipname,start45_count))
            update_dir_key(mydir, ipname, "city", cityname)
            update_dir_key(mydir, ipname, "4xxand5xx", start45_count)

    return mydir


######################################
# 首字节时延
def get_firsttime( index_today, es, manu, mydir):
    # hy is s,huawei and zte is ms
    msflag = 1000
    if (manu == "杭研"):
        msflag = 1
    if (manu == "中兴"):
        # 中兴ts分片问题，暂时无法统计首保时延
        return mydir

    querybody = {
        "size": 0,
        "query": {
            "bool": {
                "filter": {
                    "range": {
                        "@timestamp": {
                            "gt": "now-5m",
                            "lt": "now"
                        }
                    }
                }
            }
        },
        "aggs": {
            "citystat": {
                "terms": {"field": "city.keyword"},
                "aggs": {
                    "clientipstat": {
                        "terms": {"field": "host.name.keyword","size":2000},
                        "aggs": {
                            "firsttimestat": {
                                "avg": {"field": "firsttime"}
                            }
                        }
                    }
                }
            }
        }
    }
    response_byte = es.search(index=index_today, body=querybody)
    if (response_byte['hits']['total'] < 1):
        logger.info("get_firsttime hits total less to 1")
        logger.info(querybody)
        logger.info(response_byte)
        return mydir

    for bucket in response_byte['aggregations']['citystat']['buckets']:
        cityname = bucket['key']
        for i in bucket['clientipstat']['buckets']:
            ipname = i['key']
            firsttimecount = i['doc_count']
            first_avg = round(i['firsttimestat']['value'],2)
            # first_avg = round(first_avg / msflag,2)
            #print("ipname is %s,first_avg is %s" % (ipname,first_avg))
            update_dir_key(mydir, ipname, "city", cityname)
            update_dir_key(mydir, ipname, "firsttime_avg", first_avg)

    return mydir


######################################
# 平均下载速率（缓存吐出流量/服务时长），总流量趋势(缓存吐出流量)
def get_avgdown_rate(index_today, es, manu, mydir):
    # hy is s,huawei and zte is ms
    msflag = 1000
    if (manu == "杭研"):
        msflag = 1
    elif (manu == "华为"):
        msflag = 1000
    elif (manu == "中兴"):
        # 因为中兴costname无法获取，故暂不统计
        return mydir

    querybody = {
        "size": 5,
        "query": {
            "bool": {
                "filter": {
                    "range": {
                        "@timestamp": {
                            "gt": "now-5m",
                            "lt": "now"
                        }
                    }
                }
            }
        },
        "aggs": {
            "citystat": {
                "terms": {"field": "city.keyword"},
                "aggs": {
                    "clientipstat": {
                        "terms": {"field": "host.name.keyword","size":2000},
                        "aggs": {
                            "serverbytestat": {
                                "sum": {"field": "serverbyte"}
                            },
                            "costtimestat": {
                                "sum": {"field": "costtime"}
                            }
                        }
                    }
                }
            }
        }
    }
    response_byte = es.search(index=index_today, body=querybody)
    if (response_byte['hits']['total'] < 1):
        logger.info("get_avgdown_rate hits total less to 1")
        logger.info(querybody)
        logger.info(response_byte)
        return mydir

    nowutc = datetime.now(LocalTimezone())
    for bucket in response_byte['aggregations']['citystat']['buckets']:
        cityname = bucket['key']
        for i in bucket['clientipstat']['buckets']:
            ipname = i['key']
            costtime = i['costtimestat']['value']
            costtime = round(costtime / msflag, 2)
            serverbyte = i['serverbytestat']['value']
            if costtime < 0.001:
                continue
            avgdownrate = round(serverbyte * 8 / 1024 / costtime, 2)
            serverbyteGB = round(serverbyte / 1024 / 1024 / 1024, 2)
            update_dir_key(mydir, ipname, "city", cityname)
            update_dir_key(mydir, ipname, "avgdownrate", avgdownrate)
            update_dir_key(mydir, ipname, "serverbyte", serverbyteGB)
            # print("clientip is %s,avgdownrate is %s,serverbyteGB is %s" % (clientip,avgdownrate,serverbyteGB))

    return mydir

######################################
class myThreadmanu(threading.Thread):
    def __init__(self, agentid, manu, index_load, esload, index_save, essave):
        threading.Thread.__init__(self)
        self.agentid = agentid
        self.manu = manu
        self.index_load = index_load
        self.esload = esload
        self.index_save = index_save
        self.essave = essave
        self.myDir = {}
    def mergeinfo(self):
        actions = []
        pro = "广西"
        nowutc = datetime.now(LocalTimezone())
        for ipname in self.myDir:
            city = self.myDir.get(ipname).get("city")
            tscount = self.myDir.get(ipname).get("tscount")
            tsbadcount = self.myDir.get(ipname).get("tsbadcount")
            n4xxand5xx = self.myDir.get(ipname).get("4xxand5xx")
            firsttime_avg = self.myDir.get(ipname).get("firsttime_avg")
            avgdownrate = self.myDir.get(ipname).get("avgdownrate")
            serverbyte = self.myDir.get(ipname).get("serverbyte")
            #print("ipname:%s city %s, tscount: %s,tsbadcount:%s,n4xxand5xx:%s,firsttime_avg:%s,avgdownrate:%s,serverbyte:%s" % 
(ipname,city,tscount,tsbadcount,n4xxand5xx,firsttime_avg,avgdownrate,serverbyte))
            action = {
                "_index": self.index_save,
                "_type": "doc",
                "_source": {
                    "@timestamp": nowutc,
                    "province": pro,
                    "city": city,
                    "manufactor": self.manu,
                    "host": ipname,
                    "tscount": tscount,
                    "tsbadcount": tsbadcount,
                    "4xxand5xx": n4xxand5xx,
                    "firsttime_avg": firsttime_avg,
                    "avgdownrate": avgdownrate,
                    "serverbyte": serverbyte,
                }
            }
            actions.append(action)

        return actions

    def run(self):
        try:
            nowutc = datetime.now(LocalTimezone())
            # 1、质差ts(服务时长>10s)数量
            logger.info("Start get_ts_stat log of %s" % self.manu)
            self.myDir = get_ts_stat(self.index_load, self.esload, self.manu, self.myDir)
            #print("self.myDir is %s" % self.myDir)


            # 2、返回码失败（4**，5**）数量
            logger.info("Start get_httpstatus_stat log of %s" % self.manu)
            self.myDir = get_httpstatus_stat(self.index_load, self.esload, self.manu, self.myDir)
            #print("self.myDir2 is %s" % self.myDir)

            # 3、首字节平均时延数据（ms首包响应时长）
            logger.info("Start get_firsttime log of %s" % self.manu)
            self.myDir = get_firsttime(self.index_load, self.esload, self.manu, self.myDir)

            # 4、平均下载速率（缓存吐出流量/服务时长），总流量趋势(缓存吐出流量)
            logger.info("Start get_avgdown_rate log of %s" % self.manu)
            self.myDir = get_avgdown_rate(self.index_load, self.esload, self.manu, self.myDir)
            #print("self.myDir3 is %s" % self.myDir)

            listac = self.mergeinfo()
            if len(listac):
                helpers.bulk(self.essave, listac)

            # end
        except Exception as e:
            logger.info("The exception of myThreadmanu is %s" % str(e))


######################################
if __name__ == '__main__':
    try:
        month_part = datetime.strftime(utc_dt, '%Y')
        index_save = ("index_gx_%s" % month_part)

        agentid = 1000002
        #changeme
        #esload修改为本地ELK的地址
        esload = Elasticsearch(["本地ELKIP"], port=9200)
        #essave为需上传的服务器IP 
        essave = Elasticsearch(["填写上传的IP"], port=9200)

        threads = []
        # 杭研
        manu = "杭研"
        index_load = ("hy*")
        # 创建新线程
        thread1 = myThreadmanu(agentid, manu, index_load, esload, index_save, essave)
        # 设置为后台线程，这里默认是False，设置为True之后则主线程不用等待子线程
        # thread1.setDaemon(True)
        # 开启新线程
        thread1.start()
        # 添加线程到线程列表
        threads.append(thread1)

        # 中兴
        #manu = "中兴"
        #index_load = ("zte*")
        # 创建新线程
        #thread2 = myThreadmanu(agentid, manu, index_load, esload, index_save, essave)
        # 设置为后台线程，这里默认是False，设置为True之后则主线程不用等待子线程
        # thread1.setDaemon(True)
        # 开启新线程
        #thread2.start()
        # 添加线程到线程列表
        #threads.append(thread2)

        # 华为
        manu = "华为"
        index_load = ("huawei*")
        # 创建新线程
        thread3 = myThreadmanu(agentid, manu, index_load, esload, index_save, essave)
        # 设置为后台线程，这里默认是False，设置为True之后则主线程不用等待子线程
        # thread1.setDaemon(True)
        # 开启新线程
        thread3.start()
        # 添加线程到线程列表
        threads.append(thread3)

        # 等待所有线程完成
        for t in threads:
            t.join()

        print("Exiting Main Thread")

        logger.info("End print log")
    except Exception as e:
        #print('失败原因：' + str(e))
        logger.info(str(e))
