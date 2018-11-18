# encoding: UTF-8

'''
本文件中包含的是CTA模块的回测引擎，回测引擎的API和CTA引擎一致，
可以使用和实盘相同的代码进行回测。
'''

from __future__ import division
import imp
from collections import OrderedDict as OD
import sys
import os
from time import time, localtime, strftime
from datetime import datetime, timedelta

from ctaBase import *
from vnbt_utils import *
from ctaLineBar import *

class HdTickLoader(object):
    # ----------------------------------------------------------------------
    def __init__(self, DB_App, Trade_Dates = None):
        self.DB_App = DB_App
        self.Trade_Dates = Trade_Dates

        self.Price_Dict = {}

        self.curDateTime = None  # 当前Tick时间
        self.curTick = None  # 最新的tick

        self.Leg1Symbol = EMPTY_STRING
        self.Leg2Symbol = EMPTY_STRING
        self.lastLeg1Tick = None
        self.lastLeg2Tick = None
        self.SpdSymbol = EMPTY_STRING
        self.SpdSet = {'leg1x': 1, 'leg2x': 1}
        self.Fctr_Name_List = self.DB_App.get_maximal_fctr_name_list_in_markets(Market_List=['F'], Freq_Type='Intra')

    # ----------------------------------------------------------------------
    def get_Bars(self, period, symbol_List, startDate='2016-03-08', endDate='2016-03-10'):
        print 'get_Bars'
        Start_Time = datetime.now()
        Dates_Dt = self.Trade_Dates[self.Trade_Dates >= startDate]
        Dates_Dt = Dates_Dt[Dates_Dt < endDate]
        Date_List = Dates_Dt.tolist()
        Data_Dict = OD()
        sys.path.insert(0, self.DB_App.Rt_Dir + '/code/utils')
        import db_utils
        for symbol in symbol_List:
            variety = re.search('[A-Za-z]*', symbol).group()
            if period == 'd':
                Fctr_Name_List = self.DB_App.DB_Struct['CHN_F']['I']['QuoteFctr']['Inter']
                Curr_Data= self.DB_App.load_by_fctr_list(Fctr_Name_List, 'd', Time_Param = [startDate, endDate], Col_Param = [symbol], Region='CHN', Market='F', Variety_Key=variety)
                for fctr in Curr_Data:
                    Curr_Data[fctr].rename(columns={symbol: fctr}, inplace=True)
                df = pd.concat(Curr_Data.values(), axis=1, join='outer')
                df.index = map(lambda x: x.replace('_', '-'), df.index)
                # df.index = pd.DatetimeIndex(df.index)
                Data_Dict[symbol] = df
            else:
                datas = []
                for date in Date_List:
                    Curr_Source = db_utils.get_source_instance(self.DB_App.Rt_Dir, 'CHN_F_GTA_Intra_DS')
                    Curr_Data = Curr_Source.load([date], None, period, Market='F', Variety_Key=variety, Contract_Key=symbol).iloc[0, :, :]
                    datas.append(Curr_Data)
                Data_Dict[symbol] = pd.concat(datas)
        print('get_Bars Time elapsed：{0}s'.format(str((datetime.now() - Start_Time))))
        return pd.Panel(Data_Dict)
    def onBar(self, lastbar):
        pass

    # ----------------------------------------------------------------------
    def adj_Spd(self, newspdxset={'leg1x': 1, 'leg2x': 1}):
        print 'adj_Spd'
        Start_Time = datetime.now()
        self.SpdSet['leg1x'] = newspdxset['leg1x']
        self.SpdSet['leg2x'] = newspdxset['leg2x']
        self.SpdSymbol = 'Spd_' + self.Leg1Symbol + '&' + self.Leg2Symbol
        linebarset = {}
        linebarset['name'] = u'M5'
        linebarset['barTimeInterval'] = 60 * 1
        linebarset['inputBollLen'] = 20
        linebarset['inputBollStdRate'] = 1.5
        # linebarset['minDiff'] = self.minDiff
        # linebarset['shortSymbol'] = symbol1
        self.Spd_Bars = CtaLineBar(self, self.onBar, linebarset)
        self.Spd_Bars.vtSymbol = self.SpdSymbol
        for dtStr in self.Spd_Ticks.keys():
            if not (dtStr in self.Leg_Ticks[self.Leg1Symbol] and dtStr in self.Leg_Ticks[self.Leg2Symbol]):
                del self.Spd_Ticks[dtStr]
                continue
            spread_tick = self.__combine2Tick(self.Leg_Ticks[self.Leg1Symbol][dtStr], self.Leg_Ticks[self.Leg2Symbol][dtStr])
            if spread_tick is None:
                del self.Spd_Ticks[dtStr]
                continue

            spread_tick.lastPrice = self.lastLeg1Tick.lastPrice * self.SpdSet['leg1x'] - self.lastLeg2Tick.lastPrice * self.SpdSet['leg2x']
            self.Spd_Ticks[dtStr] = spread_tick
            self.Spd_Bars.onTick(spread_tick)

        print('adj_Spd Time elapsed：{0}s'.format(str((datetime.now() - Start_Time))))
    # ----------------------------------------------------------------------
    def get_Daily_Ticks(self, symbol1='IF1603', symbol2='IH1603', date='2016-03-10', spdxset={'leg1x': 1, 'leg2x': 1}):
        self.Leg1Symbol = symbol1
        self.Leg2Symbol = symbol2
        self.Leg_Ticks = {symbol1: OD(), symbol2: OD()}

        self.SpdSymbol = 'Spd_' + self.Leg1Symbol + '&' + self.Leg2Symbol
        self.SpdSet['leg1x'] = spdxset['leg1x']
        self.SpdSet['leg2x'] = spdxset['leg2x']
        self.Spd_Ticks  = OD()

        # 创建的K线设置---------------------------------------------------------------------
        linebarset = {}
        linebarset['name'] = u'M5'
        linebarset['barTimeInterval'] = 60 * 1
        linebarset['inputBollLen'] = 20
        linebarset['inputBollStdRate'] = 1.5
        # linebarset['minDiff'] = self.minDiff
        # linebarset['shortSymbol'] = symbol1

        #---------------------------------------------------------------------------------
        self.Leg1_lineMb = CtaLineBar(self, self.onBar, linebarset)
        self.Leg2_lineMb = CtaLineBar(self, self.onBar, linebarset)
        self.Leg1_lineMb.vtSymbol = symbol1
        self.Leg2_lineMb.vtSymbol = symbol2
        self.Leg_Bars    = {symbol1: self.Leg1_lineMb, symbol2: self.Leg2_lineMb}
        self.Spd_Bars    = CtaLineBar(self, self.onBar, linebarset)
        self.Spd_Bars.vtSymbol = self.SpdSymbol

        Daily_Dict = {date.replace('-', '_'): {'F': [symbol1, symbol2]}}
        self.DB_App.create_tick_data_stream(Daily_Dict)
        self.DB_App.add_tick_item_from_market(self.Fctr_Name_List, 'T', Market='F')

        ################################
        Start_Time = datetime.now()
        self.DB_App.Tick_Data_Stream.start()
        while self.DB_App.Tick_Data_Stream.isRunning():
            # time.sleep(5)
            Curr_Data = self.DB_App.Tick_Data_Stream.get()  # ****** not fill    fillna(method = 'ffill')

            if not Curr_Data.empty:
                Curr_Date = Curr_Data.major_axis[-1].split(' ')[0]
                print 'Load ticks from date: ', Curr_Date
                # self.begin_of_day(Curr_Date)

                map(lambda x: self.newTick(Curr_Data.loc[:, x, :], x, [symbol1, symbol2]), Curr_Data.major_axis)
                # self.end_of_day(Curr_Date)

        self.DB_App.Tick_Data_Stream.stop()
        print('Time elapsed：{0}s'.format(str((datetime.now() - Start_Time))))
        ################################

    # ----------------------------------------------------------------------
    def get_Days_Ticks(self, symbol1='IF1603', symbol2='IH1603', startDate='2016-03-10', endDate='2016-03-10', spdxset={'leg1x': 1, 'leg2x': 1}):
        self.Leg1Symbol = symbol1
        self.Leg2Symbol = symbol2
        self.Leg_Ticks = {symbol1: OD(), symbol2: OD()}

        self.SpdSymbol = 'Spd_' + self.Leg1Symbol + '&' + self.Leg2Symbol
        self.SpdSet['leg1x'] = spdxset['leg1x']
        self.SpdSet['leg2x'] = spdxset['leg2x']
        self.Spd_Ticks  = OD()

        # 创建的K线设置---------------------------------------------------------------------
        linebarset = {}
        linebarset['name'] = u'M'
        linebarset['barTimeInterval'] = 60 * 1
        linebarset['inputBollLen'] = 20
        linebarset['inputBollStdRate'] = 1.5
        # linebarset['minDiff'] = self.minDiff
        # linebarset['shortSymbol'] = symbol1

        #---------------------------------------------------------------------------------
        self.Leg1_lineMb = CtaLineBar(self, self.onBar, linebarset)
        self.Leg2_lineMb = CtaLineBar(self, self.onBar, linebarset)
        self.Leg1_lineMb.vtSymbol = symbol1
        self.Leg2_lineMb.vtSymbol = symbol2
        self.Leg_Bars  = {symbol1: self.Leg1_lineMb, symbol2: self.Leg2_lineMb}
        self.Spd_Bars   = CtaLineBar(self, self.onBar, linebarset)
        self.Spd_Bars.vtSymbol = self.SpdSymbol
        if self.Trade_Dates is None:
            print 'Trade_Dates is None'
            return
        Dates_Dt = self.Trade_Dates[self.Trade_Dates >= startDate]
        Dates_Dt = Dates_Dt[Dates_Dt < endDate]

        Index_Dict = OD()
        for date in Dates_Dt:
            Index_Dict[date.replace('-', '_')] = {'F': [symbol1, symbol2]}

        self.DB_App.create_tick_data_stream(Index_Dict)
        self.DB_App.add_tick_item_from_market(self.Fctr_Name_List, 'T', Market='F')

        ################################
        print 'get_Days_Ticks'
        Start_Time = datetime.now()
        self.DB_App.Tick_Data_Stream.start()
        while self.DB_App.Tick_Data_Stream.isRunning():
            # time.sleep(5)
            Curr_Data = self.DB_App.Tick_Data_Stream.get()  # ****** not fill    fillna(method = 'ffill')

            if not Curr_Data.empty:
                Curr_Date = Curr_Data.major_axis[-1].split(' ')[0]
                print 'Load ticks from date: ', Curr_Date
                # self.begin_of_day(Curr_Date)
                map(lambda x: self.newTick(Curr_Data.loc[:, x, :], x, [symbol1, symbol2]), Curr_Data.major_axis)
                # self.end_of_day(Curr_Date)

        self.DB_App.Tick_Data_Stream.stop()
        print('Time elapsed：{0}s'.format(str((datetime.now() - Start_Time))))
        ################################
    # ----------------------------------------------------------------------
    def get_Dom_Ticks(self, leg1={'Variety': 'IF', 'Dom_Set': 1}, leg2= {'Variety': 'IH', 'Dom_Set': 1}, startDate='2016-03-10', endDate='2016-03-11', spdxset={'leg1x': 1, 'leg2x': 1}):
        # 只保存最近一日的ticks
        # 只保存最近一日的spdticks

        # '''
        if self.Trade_Dates is None:
            print 'Trade_Dates is None'
            return
        Dates_Dt = self.Trade_Dates[self.Trade_Dates >= startDate]
        Dates_Dt = Dates_Dt[Dates_Dt < endDate]
        self.SpdSet['leg1x'] = spdxset['leg1x']
        self.SpdSet['leg2x'] = spdxset['leg2x']

        Index_Dict = OD()
        Leg_Dom_Dict = OD()
        for date in Dates_Dt:
            Dom = 'DomContract2' if leg1['Dom_Set'] == 2 else 'DomContract'
            leg1_Dom = self.DB_App.load_domContract_data_by_date(date, {'d': [leg1['Variety']]}, Fctr_Name=Dom, Adjusted= False)
            Leg_Dom_Dict[date] = {'leg1': leg1_Dom['d'].axes[0][0]}
            Dom = 'DomContract2' if leg2['Dom_Set'] == 2 else 'DomContract'
            leg2_Dom = self.DB_App.load_domContract_data_by_date(date, {'d': [leg2['Variety']]}, Fctr_Name=Dom, Adjusted=False)
            Leg_Dom_Dict[date]['leg2'] = leg2_Dom['d'].axes[0][0]

            Index_Dict[date.replace('-', '_')] = {'F': Leg_Dom_Dict[date].values()}

        self.DB_App.create_tick_data_stream(Index_Dict)
        self.DB_App.add_tick_item_from_market(self.Fctr_Name_List, 'T', Market='F')

        self.Leg_Bars_Dic = OD()
        self.Spd_Bars_Dic = OD()

        # 创建的K线设置---------------------------------------------------------------------
        linebarset = {}
        linebarset['name'] = u'M5'
        linebarset['barTimeInterval'] = 60 * 1
        linebarset['inputBollLen'] = 20
        linebarset['inputBollStdRate'] = 1.5
        # linebarset['minDiff'] = self.minDiff
        # linebarset['shortSymbol'] = symbol1

        ################################
        Start_Time = datetime.now()
        self.DB_App.Tick_Data_Stream.start()

        while self.DB_App.Tick_Data_Stream.isRunning():           
            Curr_Data = self.DB_App.Tick_Data_Stream.get()  # ****** not fill    fillna(method = 'ffill')

            if not Curr_Data.empty:
                Curr_Date = Curr_Data.major_axis[-1].split(' ')[0]
                print 'Curr_Date:', Curr_Date
                # self.begin_of_day(Curr_Date)

                self.Leg1Symbol = Leg_Dom_Dict[Curr_Date]['leg1']
                self.Leg2Symbol = Leg_Dom_Dict[Curr_Date]['leg2']
                self.SpdSymbol = 'Spd_' + self.Leg1Symbol + '&' + self.Leg2Symbol
                self.Leg_Ticks  = {self.Leg1Symbol: OD(), self.Leg2Symbol: OD()}
                self.Spd_Ticks = OD()

                self.Leg1_lineMb = CtaLineBar(self, self.onBar, linebarset)
                self.Leg2_lineMb = CtaLineBar(self, self.onBar, linebarset)
                self.Leg1_lineMb.vtSymbol = self.Leg1Symbol
                self.Leg2_lineMb.vtSymbol = self.Leg2Symbol

                self.Leg_Bars = {self.Leg1Symbol: self.Leg1_lineMb, self.Leg2Symbol: self.Leg2_lineMb}
                self.Spd_Bars = CtaLineBar(self, self.onBar, linebarset)
                self.Spd_Bars = self.SpdSymbol
                syblist = strip_dict(Index_Dict[Curr_Date.replace('-', '_')])
                map(lambda x: self.newTick(Curr_Data.loc[:, x, :], x, syblist), Curr_Data.major_axis)

                self.Leg_Bars_Dic[Curr_Date] = self.Leg_Bars
                self.Spd_Bars_Dic[Curr_Date] = self.Spd_Bars

                # self.end_of_day(Curr_Date)


        self.DB_App.Tick_Data_Stream.stop()
        print('Time elapsed：{0}s'.format(str((datetime.now() - Start_Time))))
        ################################
        # '''
    # ----------------------------------------------------------------------
    def newTick(self, Tick_Slice, Tick_Time, Leg_Name_List):
        # 非标准合约做法
        dtm = datetime.strptime(Tick_Time, '%Y-%m-%d %H:%M:%S %f')
        for Symbol in Leg_Name_List:
            # **************************************
            # 全是np.nan的tick记录不进行数据推送
            Curr_Tick = Tick_Slice.loc[Symbol, :]
            if not (Curr_Tick.dropna(
                    how='all').empty or Curr_Tick.AskPriceOne > 9000000.0 or Curr_Tick.BidPriceOne > 9000000.0 or Curr_Tick.AskPriceOne == 0 or Curr_Tick.BidPriceOne == 0):
                # print Tick_Time, (Symbol, Curr_Tick.LastPrice)
                # ***************************************
                Curr_Tick = Tick_Slice.loc[Symbol, :]
                curtick = CtaTickData()
                curtick.vtSymbol = Symbol  # 合约代码
                curtick.date = Tick_Time[:10]
                curtick.time = Tick_Time[11:]
                curtick.dt = dtm
                curtick.lastPrice = Curr_Tick.LastPrice  # 最新成交价
                curtick.volume = Curr_Tick.Volume  # 最新成交量
                curtick.openInterest = Curr_Tick.OpenInterest  # 持仓量
                curtick.datetime = dtm  # python的datetime时间对象
                curtick.bidPrice1 = Curr_Tick.BidPriceOne
                curtick.askPrice1 = Curr_Tick.AskPriceOne
                curtick.bidVolume1 = Curr_Tick.BidVolumeOne
                curtick.askVolume1 = Curr_Tick.AskVolumeOne

                if 'TD' in Symbol:
                    # print Tick_Time , Symbol, 'Curr_Tick.AskPriceTwo=', Curr_Tick.AskPriceTwo, 'Curr_Tick.BidPriceTwo=', Curr_Tick.BidPriceTwo
                    curtick.bidPrice2 = Curr_Tick.BidPriceTwo
                    curtick.askPrice2 = Curr_Tick.AskPriceTwo
                    curtick.bidVolume2 = Curr_Tick.BidVolumeTwo
                    curtick.askVolume2 = Curr_Tick.AskVolumeTwo

                if Curr_Tick.LastPrice == Curr_Tick.LastPrice:
                    self.Price_Dict[Symbol] = Curr_Tick.LastPrice
                    print 'newTick: ', Tick_Time, (Symbol, Curr_Tick.LastPrice)
                    if (curtick.datetime.hour >= 3 and curtick.datetime.hour <= 8) or (curtick.datetime.hour >= 16 and curtick.datetime.hour <= 20):
                        return

                    # ------缓存tick并生成bar, 生成spdtick并合成spdbar
                    self.onTick(curtick)

    #----------------------------------------------------------------------
    def onTick(self, tick):
        """行情更新
        :type tick: object
        """
        self.curTick = tick
        self.curDateTime = tick.datetime
        Symbol = tick.vtSymbol
        dtStr = ' '.join([tick.date, tick.time])
        self.Leg_Ticks[Symbol][dtStr] = tick
        #--在onTick()中合成bar
        self.Leg_Bars[Symbol].onTick(tick)

        spread_tick = None
        # 合并spd tick
        if tick.vtSymbol != EMPTY_STRING:
            spread_tick = self.__combineTick(tick)
        if spread_tick is None:
            return

        # # 修正lastPrice，大于中轴(0)时，取最小值，小于中轴时，取最大值
        # if spread_tick.bidPrice1 > self.baseMidLine and spread_tick.askPrice1 > self.baseMidLine:
        #     spread_tick.lastPrice = min(spread_tick.bidPrice1, spread_tick.askPrice1)
        # elif spread_tick.bidPrice1 < self.baseMidLine and spread_tick.askPrice1 < self.baseMidLine:
        #     spread_tick.lastPrice = max(spread_tick.bidPrice1, spread_tick.askPrice1)

        spread_tick.lastPrice = self.lastLeg1Tick.lastPrice * self.SpdSet['leg1x'] - self.lastLeg2Tick.lastPrice * self.SpdSet['leg2x']
        dtStr = ' '.join([spread_tick.date, spread_tick.time])
        self.Spd_Ticks[dtStr] = spread_tick
        self.Spd_Bars.onTick(spread_tick)
    # ----------------------------------------------------------------------
    def __combineTick(self, tick):
        """合并两腿合约，成为套利合约"""
        combinable = False

        if tick.vtSymbol == self.Leg1Symbol:
            # leg1合约
            self.lastLeg1Tick = tick
            if self.lastLeg2Tick is not None:
                if self.lastLeg1Tick.datetime == self.lastLeg2Tick.datetime:
                    combinable = True
        elif tick.vtSymbol == self.Leg2Symbol:
            # leg2合约
            self.lastLeg2Tick = tick
            if self.lastLeg1Tick is not None:
                if self.lastLeg2Tick.datetime == self.lastLeg1Tick.datetime:
                    combinable = True

        # 不能合并
        if not combinable:
            return None

        spread_tick = CtaTickData()
        spread_tick.vtSymbol = self.SpdSymbol
        spread_tick.symbol = self.SpdSymbol

        spread_tick.datetime = tick.datetime
        spread_tick.date = tick.date
        spread_tick.time = tick.time

        # 以下情况，基本为单腿涨跌停，不合成价差Tick
        if (self.lastLeg1Tick.askPrice1 > 9000000 or self.lastLeg1Tick.askPrice1 == 0 or self.lastLeg1Tick.bidPrice1 == self.lastLeg1Tick.upperLimit) and self.lastLeg1Tick.askVolume1 == 0:
            self.writeCtaLog(u'leg1:{0}涨停{1}，不合成价差Tick'.format(self.lastLeg1Tick.vtSymbol, self.lastLeg1Tick.bidPrice1))
            return None
        if (self.lastLeg1Tick.bidPrice1 > 9000000 or self.lastLeg1Tick.bidPrice1 == 0 or self.lastLeg1Tick.askPrice1 == self.lastLeg1Tick.lowerLimit) and self.lastLeg1Tick.bidVolume1 == 0:
            self.writeCtaLog(u'leg1:{0}跌停{1}，不合成价差Tick'.format(self.lastLeg1Tick.vtSymbol, self.lastLeg1Tick.askPrice1))
            return None
        if (self.lastLeg2Tick.askPrice1 > 9000000 or self.lastLeg2Tick.askPrice1 == 0 or self.lastLeg2Tick.bidPrice1 == self.lastLeg2Tick.upperLimit) and self.lastLeg2Tick.askVolume1 == 0:
            self.writeCtaLog(u'leg2:{0}涨停{1}，不合成价差Tick'.format(self.lastLeg2Tick.vtSymbol, self.lastLeg2Tick.bidPrice1))
            return None
        if (self.lastLeg2Tick.bidPrice1 > 9000000 or self.lastLeg2Tick.bidPrice1 == 0 or self.lastLeg2Tick.askPrice1 == self.lastLeg2Tick.lowerLimit) and self.lastLeg2Tick.bidVolume1 == 0:
            self.writeCtaLog(u'leg2:{0}跌停{1}，不合成价差Tick'.format(self.lastLeg2Tick.vtSymbol, self.lastLeg2Tick.askPrice1))
            return None

        # 叫卖价差=leg1.askPrice1 * x1 - leg2.bidPrice1 * x2，volume为两者最小, 折算到leg1上
        spread_tick.askPrice1 = self.lastLeg1Tick.askPrice1 * self.SpdSet['leg1x'] - self.lastLeg2Tick.bidPrice1 * self.SpdSet['leg2x']
        if self.lastLeg1Tick.askVolume1 * self.SpdSet['leg2x'] <= self.lastLeg2Tick.bidVolume1 * self.SpdSet['leg1x']:
            spread_tick.askVolume1 = self.lastLeg1Tick.askVolume1
        else:
            spread_tick.askVolume1 = self.lastLeg2Tick.bidVolume1 * self.SpdSet['leg1x'] / self.SpdSet['leg2x']
        # spread_tick.askVolume1 = min(self.lastLeg1Tick.askVolume1, self.lastLeg2Tick.bidVolume1)

        # 叫买价差=leg1.bidPrice1 - leg2.askPrice1，volume为两者最小,折算到leg1上
        spread_tick.bidPrice1 = self.lastLeg1Tick.bidPrice1 * self.SpdSet['leg1x'] - self.lastLeg2Tick.askPrice1 * self.SpdSet['leg2x']
        if self.lastLeg1Tick.bidVolume1 * self.SpdSet['leg2x'] <= self.lastLeg2Tick.askVolume1 * self.SpdSet['leg1x']:
            spread_tick.bidVolume1 = self.lastLeg1Tick.bidVolume1
        else:
            spread_tick.bidVolume1 = self.lastLeg2Tick.askVolume1 * self.SpdSet['leg1x'] / self.SpdSet['leg2x']
        # spread_tick.bidVolume1 = min(self.lastLeg1Tick.bidVolume1, self.lastLeg2Tick.askVolume1)

        return spread_tick

    # ----------------------------------------------------------------------
    # ----------------------------------------------------------------------
    def __combine2Tick(self, leg1tick, leg2tick):
        """合并两腿合约，成为套利合约"""

        self.lastLeg1Tick = leg1tick
        self.lastLeg2Tick = leg2tick
        self.Leg1Symbol = leg1tick.vtSymbol
        self.Leg2Symbol = leg2tick.vtSymbol

        spread_tick = CtaTickData()
        spread_tick.vtSymbol = self.SpdSymbol
        spread_tick.symbol = self.SpdSymbol

        spread_tick.datetime = self.lastLeg1Tick.datetime
        spread_tick.date = self.lastLeg1Tick.date
        spread_tick.time = self.lastLeg1Tick.time

        # 以下情况，基本为单腿涨跌停，不合成价差Tick
        if (
                self.lastLeg1Tick.askPrice1 > 9000000 or self.lastLeg1Tick.askPrice1 == 0 or self.lastLeg1Tick.bidPrice1 == self.lastLeg1Tick.upperLimit) and self.lastLeg1Tick.askVolume1 == 0:
            self.writeCtaLog(u'leg1:{0}涨停{1}，不合成价差Tick'.format(self.lastLeg1Tick.vtSymbol, self.lastLeg1Tick.bidPrice1))
            return None
        if (
                self.lastLeg1Tick.bidPrice1 > 9000000 or self.lastLeg1Tick.bidPrice1 == 0 or self.lastLeg1Tick.askPrice1 == self.lastLeg1Tick.lowerLimit) and self.lastLeg1Tick.bidVolume1 == 0:
            self.writeCtaLog(u'leg1:{0}跌停{1}，不合成价差Tick'.format(self.lastLeg1Tick.vtSymbol, self.lastLeg1Tick.askPrice1))
            return None
        if (
                self.lastLeg2Tick.askPrice1 > 9000000 or self.lastLeg2Tick.askPrice1 == 0 or self.lastLeg2Tick.bidPrice1 == self.lastLeg2Tick.upperLimit) and self.lastLeg2Tick.askVolume1 == 0:
            self.writeCtaLog(u'leg2:{0}涨停{1}，不合成价差Tick'.format(self.lastLeg2Tick.vtSymbol, self.lastLeg2Tick.bidPrice1))
            return None
        if (
                self.lastLeg2Tick.bidPrice1 > 9000000 or self.lastLeg2Tick.bidPrice1 == 0 or self.lastLeg2Tick.askPrice1 == self.lastLeg2Tick.lowerLimit) and self.lastLeg2Tick.bidVolume1 == 0:
            self.writeCtaLog(u'leg2:{0}跌停{1}，不合成价差Tick'.format(self.lastLeg2Tick.vtSymbol, self.lastLeg2Tick.askPrice1))
            return None

        # 叫卖价差=leg1.askPrice1 * x1 - leg2.bidPrice1 * x2，volume为两者最小, 折算到leg1上
        spread_tick.askPrice1 = self.lastLeg1Tick.askPrice1 * self.SpdSet['leg1x'] - self.lastLeg2Tick.bidPrice1 * self.SpdSet['leg2x']
        if self.lastLeg1Tick.askVolume1 * self.SpdSet['leg2x'] <= self.lastLeg2Tick.bidVolume1 * self.SpdSet['leg1x']:
            spread_tick.askVolume1 = self.lastLeg1Tick.askVolume1
        else:
            spread_tick.askVolume1 = self.lastLeg2Tick.bidVolume1 * self.SpdSet['leg1x'] / self.SpdSet['leg2x']
        # spread_tick.askVolume1 = min(self.lastLeg1Tick.askVolume1, self.lastLeg2Tick.bidVolume1)

        # 叫买价差=leg1.bidPrice1 - leg2.askPrice1，volume为两者最小,折算到leg1上
        spread_tick.bidPrice1 = self.lastLeg1Tick.bidPrice1 * self.SpdSet['leg1x'] - self.lastLeg2Tick.askPrice1 * self.SpdSet['leg2x']
        if self.lastLeg1Tick.bidVolume1 * self.SpdSet['leg2x'] <= self.lastLeg2Tick.askVolume1 * self.SpdSet['leg1x']:
            spread_tick.bidVolume1 = self.lastLeg1Tick.bidVolume1
        else:
            spread_tick.bidVolume1 = self.lastLeg2Tick.askVolume1 * self.SpdSet['leg1x'] / self.SpdSet['leg2x']
        # spread_tick.bidVolume1 = min(self.lastLeg1Tick.bidVolume1, self.lastLeg2Tick.askVolume1)

        return spread_tick

    # ----------------------------------------------------------------------
    def writeCtaLog(self, content):
        print 'hdTickLoader: ' + content




if __name__ == '__main__':
    Rt_Dir    = '/home/chenxiubin/projects/vnsa'
    DB_Rt_Dir = '/data/all/project/hdf5_database'.replace('\\', '/')
    # 初始化数据接口
    sys.path.insert(0, DB_Rt_Dir + '/code/db_app.py')
    sys.path.insert(0, DB_Rt_Dir + '/code/')
    DB_App = getattr(imp.load_source('DB_App', DB_Rt_Dir + '/code/db_app.py'), 'DB_App')(DB_Rt_Dir)

    Start_Date = '2016-01-05'
    End_Date = '2016-03-11'

    leg1 = {'Variety': 'IF', 'Dom_Set': 1}  # Dom_Set: 1 (dom1)  or  2(dom2)
    leg2 = {'Variety': 'IC', 'Dom_Set': 1}

    # vnsa_Config['Days_Shift_Before'] = 2
    # vnsa_Config['Days_Shift_After'] = 2
    Trade_Dates = pd.read_csv(Rt_Dir + '/vnpy/trader/CHN_Date.csv').iloc[:, 0]
    Hd_Loader = HdTickLoader(DB_App, Trade_Dates)
    # Hd_Loader.get_Daily_Ticks(symbol1='IF1603', symbol2='IH1603', date='2016-03-10')

    # Hd_Loader.get_Days_Ticks(symbol1='IF1603', symbol2='IH1603', startDate=Start_Date, endDate=End_Date, spdxset={'leg1x': 1, 'leg2x': 1})
    # Hd_Loader.adj_Spd(newspdxset={'leg1x': 2, 'leg2x': 3})


    # Hd_Loader.get_Dom_Ticks(leg1=leg1, leg2=leg2, startDate=Start_Date, endDate=End_Date)

    bard = Hd_Loader.get_Bars('d', ['IF1603', 'IC1603'], startDate=Start_Date, endDate=End_Date)
    print 'ok'
