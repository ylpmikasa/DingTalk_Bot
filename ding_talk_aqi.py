# -*- coding: utf-8 -*-
#author 杨丽鹏
#date 2022-02-17

import pymssql
import requests
import json
import datetime
from apscheduler.schedulers.blocking import BlockingScheduler
from dateutil.parser import parse

dic = {"510108":"成华区","510109":"高新区","510115":"温江区","510116":"天府新区","510122":"双流区","510182":"彭州市"}

def dingTalk(emo, info, picUrl):
    headers = {
        "Content-Type": "application/json"
    }
    data = {
        "msgtype": "markdown",
        "markdown": {
            "title": "行政区划预警",
            "text": "### "+emo+" 行政区划预警  \n >__各区数据更新时间如下：__ \n "+info+" > ![screenshot]("+picUrl+")\n > ###### "+datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')+" 发布 [站点数据](http://221.237.108.13:90/APSGCMIS_CQLP/Web/AqiQuery.html) \n"
        },
        "at": {
            "isAtAll": 'false'
        }
    }
    json_data = json.dumps(data)
    infoReq =  requests.post(
        #钉钉Webhook-TODO：your access_token
        #预警消息群链接
        url='https://oapi.dingtalk.com/robot/send?access_token=your access_token',
        data=json_data, headers=headers)
    infoReq.close()

class MSSQL:
    def __init__(self,host,user,pwd,db):
        self.host = host
        self.user = user
        self.pwd = pwd
        self.db = db

    def __GetConnect(self):
        if not self.db:
            raise(NameError,"没有设置数据库信息")
        self.conn = pymssql.connect(host=self.host,user=self.user,password=self.pwd,database=self.db,charset="utf8")
        cur = self.conn.cursor()
        if not cur:
            raise(NameError,"连接数据库失败")
        else:
            return cur

    def ExecQuery(self,sql):
        cur = self.__GetConnect()
        cur.execute(sql)
        resList = cur.fetchall()

        #查询完毕后必须关闭连接
        self.conn.close()
        return resList

    def ExecNonQuery(self,sql):
        cur = self.__GetConnect()
        cur.execute(sql)
        self.conn.commit()
        self.conn.close()

#SqlServer连接配置-TODO：your ip,username,password,dbname
ms = MSSQL(host="ip",user="username",pwd="password",db="dbname")

#定时任务
def scheduledTask():
    # 查询数据库-TODO：your sql
    reslist = ms.ExecQuery(
        "your sql")
    # 拼接钉钉群消息内容-TODO：自定义拼接方式
    info = ""
    emo = "&#x1F197;"
    now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    for i, j in reslist:
        differ = (parse(str(now)) - parse(str(j))).total_seconds()
        if differ > 3600:
            info += " \n "+dic.get(str(i)) + " <font color=#FF0000>" + str(j) + "</font> \n "
            emo = "&#x2757;"
        else:
            info += " \n "+dic.get(str(i)) +" "+ str(j) +" \n "
    #生成随机图片-TODO：自定义随机图片链接
    picReq = requests.get("https://api.ixiaowai.cn/gqapi/gqapi.php")
    # 发送钉钉群消息
    dingTalk(emo, info, picReq.url)
    picReq.close()
    print("执行时间："+datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

if __name__ == '__main__':
    # 启动时执行一次
    scheduledTask()
    # 定时任务
    sched = BlockingScheduler()
    # 每小时55分执行一次定时任务
    sched.add_job(scheduledTask, 'cron', hour='8-23', minute='55', misfire_grace_time=120)
    # 每5秒执行一次
    # sched.add_job(scheduledTask, 'cron', second='5')
    # 启动定时任务
    sched.start()
