# encoding: UTF-8

"""
Mat
"""

import os
import sys
import talib
import numpy as np
import pandas as pd

from vnpy.trader.app.ctaStrategy.ctaBase import *
from vnpy.trader.app.ctaStrategy.barLoader import *


def showinds( datain = 'mongo'):
    trdvar = 'RB'
    period = 'M115'
    dataStartDate = '2016-06-01'
    dataEndDate = '2017-06-01'
    DB_Rt_Dir = r'D:\ArcticFox\project\hdf5_database'.replace('\\', '/')

    vada = load_Dombar(trdvar, period, [dataStartDate, dataEndDate], Datain=datain, Host='localhost',
                       DB_Rt_Dir=DB_Rt_Dir, Dom='DomContract', Adj=True)

    Dat_bar = vada.loc[:]
    Dat_bar['TR1'] = Dat_bar['high'] - Dat_bar['low']
    Dat_bar['TR2'] = abs(Dat_bar['high'] - Dat_bar['close'].shift(1))
    Dat_bar['TR3'] = abs(Dat_bar['low'] - Dat_bar['close'].shift(1))
    TR = Dat_bar.loc[:, ['TR1', 'TR2', 'TR3']].max(axis=1)
    ATR = TR.rolling(14).mean()
    vada['Ma5'] = Dat_bar['close'].rolling(5).mean()
    vada['Ma10'] = Dat_bar['close'].rolling(10).mean()
    vada['Ma20'] = Dat_bar['close'].rolling(20).mean()
    vada['Ma30'] = Dat_bar['close'].rolling(30).mean()
    vada['Ma60'] = Dat_bar['close'].rolling(60).mean()
    vada['ATR'] = ATR.div(vada['Ma10'])

    slowk, slowd = talib.STOCH(Dat_bar['high'].values,
                               Dat_bar['low'].values,
                               Dat_bar['close'].values,
                               fastk_period=9,
                               slowk_period=3,
                               slowk_matype=0,
                               slowd_period=3,
                               slowd_matype=0)
    # 获得最近的kd值
    # slowk = slowk[-1]
    # slowd = slowd[-1]
    vada['KD_k'] = slowk
    vada['KD_d'] = slowd
    plotsdk(vada, symbol=trdvar, disfactors=['Ma5','Ma10','Ma20','Ma30','Ma60','KD_k', 'KD_d'], has2wind= True)
    print 'ok'

if __name__ == '__main__':
    showinds(datain = 'mongo')
