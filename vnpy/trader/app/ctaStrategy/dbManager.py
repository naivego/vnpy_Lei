# encoding: UTF-8
from __future__ import division

import sys
import os
from datetime import datetime, timedelta
from collections import OrderedDict
from itertools import product
import multiprocessing
import pymongo
import json
import sys
import cPickle
import csv
import logging
import copy
import pandas as pd
import re
from vnpy.trader.app.ctaStrategy.ctaBase import *
from vnpy.trader.vtConstant import *



def dbConnect(host, port, logging = True):
    """连接MongoDB数据库"""

    try:
        # 设置MongoDB操作的超时时间为0.5秒
        dbClient = pymongo.MongoClient(host, port, connectTimeoutMS=500) #

        # 调用server_info查询服务器状态，防止服务器异常并未连接成功
        dbClient.server_info()

        return dbClient

    except pymongo.errors.ConnectionFailure:
        print 'ConnectionFailure'
        return None


 # ----------------------------------------------------------------------
def dbInsert(dbClient, dbName, collectionName, d):
        """向MongoDB中插入数据，d是具体数据"""
        if dbClient:
            db = dbClient[dbName]

            collection = db[collectionName]

            collection.insert(d)
        else:
            dbClient.writeLog(text.DATA_INSERT_FAILED)

    # ----------------------------------------------------------------------
def dbQuery(dbClient, dbName, collectionName, d):
        """从MongoDB中读取数据，d是查询要求，返回的是数据库查询的指针"""
        if dbClient:
            db = dbClient[dbName]
            collection = db[collectionName]
            cursor = collection.find() #d
            if cursor:
                return list(cursor)
            else:
                return []
        else:
            #dbClient.writeLog(text.DATA_QUERY_FAILED)
            return []

    # ----------------------------------------------------------------------
def dbUpdate(dbClient, dbName, collectionName, d, flt, upsert=False):
        """向MongoDB中更新数据，d是具体数据，flt是过滤条件，upsert代表若无是否要插入"""
        if dbClient:
            db = dbClient[dbName]
            collection = db[collectionName]
            collection.replace_one(flt, d, upsert)
        else:
            dbClient.writeLog(text.DATA_UPDATE_FAILED)

 # ----------------------------------------------------------------------

class DbManager(object):
    # ----------------------------------------------------------
    def __init__(self ):
        # 连接数据库
        self.dbClient = None  # 数据库客户端
        self.dbCursor = None  # 数据库指针
        host = "localhost"
        port = 27017
        self.dbClient = dbConnect(host, port)

    # ----------------------------------------------------------
    def loadBarFromMongo(self, dbname= 'Dom_M', var='RB', startdate= '2017-01-01', enddate='2017-06-01'):
        collection = self.dbClient[dbname][var]
        collection.create_index([('datetime', pymongo.ASCENDING)])

        df = pd.DataFrame()
        # 载入初始化需要用的数据
        flt = {'datetime': {'$gte': startdate, '$lt': enddate}}
        self.dbCursor = collection.find(flt).sort('datetime', pymongo.ASCENDING)
        datas = list(self.dbCursor)
        if len(datas) == 0:
            return df
        df = pd.DataFrame(datas)
        df.drop(['_id'], axis=1, inplace= True)
        df.set_index('datetime', inplace=True)

        # for d in self.dbCursor:
        #     bar = CtaBarData()
        #     bar.__dict__ = d
        #     bars.append(bar)
        return df
    # ----------------------------------------------------------
    def makeBarFromM(self, dbname='Dom_M', var='RB', startdate='2017-01-01', enddate='2017-06-01', mkn= 'M15'):
        print 'makeBarFromM'
        df = self.loadBarFromMongo(dbname, var, startdate, enddate)
        #af = pd.DataFrame(columns=['open', 'high', 'low', 'close', 'openInterest', 'volume', 'adjFactor'])
        barlist = []
        bar_mn = int(mkn[1:])
        if bar_mn<=1:
            return
        barms = 0
        prehh = '00'
        date =''
        newbar = pd.Series()
        for dtm in df.index:
            if date != dtm.split(' ')[0]:
                date = dtm.split(' ')[0]
                print dtm
            time = dtm.split(' ')[1]
            crthh = time[:2]
            crtbar = df.loc[dtm, :].copy()

            # ---从开盘bar开始合成
            if prehh == '15' and (crthh == '09' or crthh == '21'):
                barms = 1
            else:
                if barms:
                    barms += 1
                    if barms > bar_mn:
                        barms = 1
            if barms == 1:
                if not newbar.empty:
                    # af = af.append(newbar)
                    barlist.append(newbar)
                newbar = crtbar.copy()
            elif 0 < barms <= bar_mn:

                newbar['high'] = max(newbar['high'], crtbar['high'])
                newbar['low'] = min(newbar['low'], crtbar['low'])
                newbar['close'] = crtbar['close']
                newbar['volume'] = newbar['volume'] + crtbar['volume']
                newbar['openInterest'] = crtbar['openInterest']
                newbar.name = dtm

            prehh = crthh
        af = pd.DataFrame(barlist)
        return af
    # ----------------------------------------------------------
    def saveDataToMongo(self, dbname, var, datas):
        collection= self.dbClient[dbname][var]
        for bar in datas:
            collection.insert_one(bar.__dict__)

# ----------------------------------------------------------
    def saveDfToMongo(self, dbname, var, dfdata, indexcol= 'datetime', adddt = True):
        df = dfdata.copy(deep= True)
        if indexcol:
            df[indexcol] = df.index
        if adddt:
            if 'datetime' in df.columns:  # 'date' not in df.columns and 'time' not in df.columns and
                dates = []
                times = []
                for dt in df['datetime']:
                    dtm = dt.split(' ')
                    dates.append(dtm[0])
                    try:
                        times.append(dtm[1])
                    except:
                        times.append('16:00:00')
                df['date'] = dates
                df['time'] = times

        for i in range(0, df.index.size, 100):
            sdf = df.ix[i:i+100,:]
            self.dbClient[dbname][var].insert(json.loads(sdf.T.to_json(), object_pairs_hook=OrderedDict).values())
            # self.dbClient[dbname][var].insert(df.to_dict())

# ----------------------------------------------------------
    def correctDatas(self, dbna = 'Dom_M'):
        # 自2016年5月3日起，螺纹钢、热轧卷板、石油沥青期货品种的连续交易时间由每周一至周五的21：00至次日1：00调整为21：00至23：00
        if 1:
            vars = ['RB', 'HC', 'BU']
            for var in vars:
                mycol = self.dbClient[dbna][var]
                flt = {"date": {'$gt': "2016-05-02"}, '$or': [{"time": {'$gt': "23:00:00"}}, {"time": {'$lt': "03:00:00"}}]}   #'$regex': hh
                self.dbCursor = mycol.find(flt).sort('datetime', pymongo.ASCENDING)
                datas = list(self.dbCursor)
                if len(datas) > 0:
                    pass
                    x = mycol.delete_many(flt)
                    print(x.deleted_count, " is to be del")

# ----------------------------------------------------------
def csvtodb():
    dbm = DbManager()
    periods = ['M30']  # 'd', 'M', 'M30'
    vars = ['IF', 'AU', 'RB']
    for period in periods:
        domdir = r'D:\lab\xDom' + '/' + period
        for var in vars:
            csvfile = os.path.join(domdir, var + '_' + period + '.csv')
            df = pd.read_csv(csvfile, index_col=0, encoding='gbk')
            dbm.saveDfToMongo('Dom_' + period, var, df)

# ----------------------------------------------------------
def opnbarpros(var, bars_df):
    af = pd.DataFrame(columns=['open', 'high','low','close','openInterest','volume','adjFactor'])
    barlist = []
    presta = 0 # bar状态 0-wz 1- 开盘竞价bar 2-盘中连续交易bar 3-收盘bar
    date = ''
    prehh = '15'
    prebar = None
    for dtm in bars_df.index:
        if date != dtm.split(' ')[0]:
            date = dtm.split(' ')[0]
            print dtm

        time = dtm.split(' ')[1]
        crthh = time[:2]
        crtbar = bars_df.loc[dtm,:].copy()
        #---判断当前bar是否为开盘集合竞价bar
        if prehh == '15' and (crthh =='09' or crthh =='21'):
            if var in MARKET_ZJ:
                if time == '09:15:00' or time == '09:30:00':
                    crtsta = 1
                else:
                    crtsta = 2
            else:
                if time == '09:00:00' or time == '21:00:00':
                    crtsta = 1
                else:
                    crtsta = 2
        else:
            crtsta = 2

        if presta==1:
            # 之前是开盘竞价bar，合并到当前bar中
            crtbar['open'] = prebar['open']
            crtbar['high'] = max(prebar['high'], crtbar['high'])
            crtbar['low'] = min(prebar['low'], crtbar['low'])
            crtbar['volume'] = crtbar['volume'] + prebar['volume']

        if crtsta > 1:
            barlist.append(crtbar)
            # af = af.append(crtbar)
        presta = crtsta
        prebar = crtbar.copy()
        prehh  = crthh
    af = pd.DataFrame(barlist)
    return af

# ----------------------------------------------------------
def stdbard(pathdir, savedir, period, vars):
    # csv源文件目录
    # pathdir = r'D:\lab\Dom\M'

    # 另保存目录
    # savedir =r'D:\lab\xDom\M30'

    pathdir = pathdir + '/'+ period
    savedir = savedir + '/'+ period

    if not (os.path.exists(savedir)):
        os.makedirs(savedir)

    for root, dirs, files in os.walk(pathdir):
        for file in files:
            if '.csv' in file and file.split('_')[0] in vars :
                csvfile = os.path.join(root, file)
                df = pd.read_csv(csvfile, index_col=0, encoding='gbk')
                if 0:
                    if period == 'd':
                        df.drop(['adjFactor_d'], axis=1, inplace= True)
                    else:
                        df.drop(['adjFactor'], axis=1, inplace=True)

                # 修改索引名称
                if period == 'd':
                    reindex = [date.replace('_', '-') for date in df.index]
                    df.index= reindex

                # 如果需要，修改列名 去掉 '_d'
                if 1:
                    recol = {col: col.replace('_'+period, '') for col in df.columns}
                    df.rename(columns= recol, inplace=True)

                # 修改索引列的表头名称
                if 0:
                    df.index.rename('data', inplace=True)

                df.to_csv(savedir+'/'+file, encoding='gbk')
                print file, 'ok'


if __name__ == '__main__':
    # stdbard(r'D:\lab\Dom', r'D:\lab\xDom', 'M', ['IC', 'J'])
    # csvtodb()

    print 'start time: ', datetime.now()
    dbm = DbManager()

    var_List_all = ['Y', 'FU', 'BB', 'ZN', 'JR', 'WH', 'BU', 'FB', 'WR', 'FG', 'JD', 'HC', 'L', 'NI',
                    'PP', 'RS', 'PB', 'LR', 'TF', 'RM', 'RI', 'PM', 'A', 'C', 'B', 'AG', 'RU', 'I', 'J',
                    'M', 'AL', 'CF', 'IH', 'AU', 'T', 'V', 'CS', 'IC', 'CU', 'IF', 'MA', 'OI', 'JM', 'SR', 'SF',
                    'SN', 'SM', 'RB', 'TA', 'P', 'ZC']

    # ------------------------从csv生成Bar_M并存入mogodb
    #----------------------------------------------------
    if 0:
        periods =['M', 'M5', 'M15', 'M30', 'H', 'd']  # ['M'] #
        vars = ['J','IF','IC','IH','TA','RB','I','CU']  # ['RB']#
        for period in periods:
            domdir = r'D:\lab\Domnew' + '/' + period
            for var in vars:
                print 'saveDfToMongo for: ', var, ' period:', period
                csvfile = os.path.join(domdir, var + '_' + period + '.csv')
                df = pd.read_csv(csvfile, index_col=0, encoding='gbk')
                if 0:   # #----剔除集合竞价bar
                    af = opnbarpros(var, df)
                else:
                    af = df
                af['vtSymbol'] = var

                dbm.saveDfToMongo('Dom_' + period, var, af)
    #----------------------------------------------------
    if 0:
        for T in ['M', 'M5', 'M15', 'M30', 'H']:
            dbm.correctDatas(dbna='_'.join(['Dom', T]))

    # ------------------------从mongodb读取Bar_M并合成多周期bar并入库
    if 0:
        makebarconfig = {
            # 'M15': ['RB'],
            # 'M30': ['RB'], #'V'
            # 'M60': ['RB'],

            # 'M75': ['TA', 'RB'],
            # 'M111': ['AU'],
            # 'M115': ['RB'],
            'M125': ['RB'],
            # 'M155': ['CU'],
        }
        db_bar_ms= dbm.dbClient['Dom_M'].collection_names()
        for mkn, vars in makebarconfig.iteritems():
            for var in vars:
                if var not in db_bar_ms:
                    print 'error! ', var, ' ont in Dom_M'
                    continue
                print 'make bar for', var, mkn
                af = dbm.makeBarFromM(dbname='Dom_M', var=var, startdate='2010-01-01', enddate='2017-06-31', mkn=mkn)
                dbm.saveDfToMongo('Dom_'+mkn, var, af)
                af.to_csv(r'D:\lab\Dom\mkbars' + '/' + var+'_'+mkn + '.csv', encoding='gbk')


    print 'end time: ', datetime.now()

