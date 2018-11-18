# encoding: UTF-8

from __future__ import division

import os
import importlib
import csv
import copy
import re
import imp
import numpy  as np
import pandas as pd
import pickle as pk
import matplotlib.pyplot as plt

########################################################################
# ----------------------------------------------------------------------
def max_drawdown(Networth_Series):
    '''
    计算最大回撤相关的评价指标
    :param Networth_Series: pd.Series, 净值序列
    :return: {'Max':净值最高点, 'Max_Drawdown':最大回撤, 'Drawdown_Duration':回撤周期， 'Recovery_Duration':回复周期}
    '''
    Series            = Networth_Series[:-1]
    Size              = Series.size
    Max               = np.zeros(Size)
    Drawdown          = np.zeros(Size)
    Drawdown_Duration = np.zeros(Size)
    Recovery_Duration = None
    Max_Drawdown      = 0
    Ind               = 0

    for t in range(Size):
        Max[t]      = max(Max[t-1], Series[t])
        Drawdown[t] = float(Series[t] - Max[t])/Max[t]
        if Drawdown[t] < Max_Drawdown:
            Max_Drawdown = Drawdown[t]
            Ind          = t
        Drawdown_Duration[t] = (0 if Drawdown[t] >= 0 else Drawdown_Duration[t-1] + 1)

    if Ind is not 0:
        Temp_Max = map(lambda x:x - Max[Ind], Max[Ind:])
        for i in range(len(Max[Ind:])):
            if Temp_Max[i] > 0:
                Recovery_Duration = i
                break

    return {'Max':Max, 'Max_Drawdown':Drawdown[Ind], 'Drawdown_Duration':Drawdown_Duration[Ind], 'Recovery_Duration':Recovery_Duration}

# ----------------------------------------------------------------------
def return_metrics(Networth_Series, Trading_Days_Per_Year = 242):
    '''
    计算收益相关评价指标
    :param Networth_Series: pd.Series, 净值序列
    :param Trading_Days_Per_Year:
    :return: {'Daily_Return':每日盈亏, 'Annualized_Return':年化收益, 'Annualized_Std':年化标准差, 'Sharpe_Ratio':夏普比, 'Sortino_Ratio':索提诺比, 'Winning_Rate':日胜率, 'Num_Days':交易天数}
    '''
    Num_Days     = Networth_Series.size
    Daily_Return = (Networth_Series / Networth_Series.shift() - 1)[1:]
    Winning_Rate = Daily_Return[Daily_Return > 0].size / Daily_Return.dropna().size

    Mean_Daily_Return = Daily_Return.mean()
    Annualized_Return = np.power((Mean_Daily_Return + 1), Trading_Days_Per_Year) - 1
    Annualized_Std    = Daily_Return.std() * np.sqrt(Trading_Days_Per_Year)
    Sharpe_Ratio      = Annualized_Return / Annualized_Std
    Sortino_Ratio     = Annualized_Return / ( Daily_Return[Daily_Return < 0].std() * np.sqrt(Trading_Days_Per_Year) )

    return {'Daily_Return':Daily_Return, 'Annualized_Return':Annualized_Return, 'Annualized_Std':Annualized_Std, 'Sharpe_Ratio':Sharpe_Ratio, 'Sortino_Ratio':Sortino_Ratio, 'Winning_Rate':Winning_Rate, 'Num_Days':Num_Days}

# ----------------------------------------------------------------------
def calc_metrics(Networth_Series, Trading_Days_Per_Year = 242):
    '''
    计算收益和回撤评价指标
    :param Networth_Series: pd.Series, 净值序列
    :param Trading_Days_Per_Year:
    :return: {'Max':净值最高点, 'Max_Drawdown':最大回撤, 'Drawdown_Duration':回撤周期， 'Recovery_Duration':回复周期，
    'Daily_Return':每日盈亏, 'Annualized_Return':年化收益, 'Annualized_Std':年化标准差, 'Sharpe_Ratio':夏普比, 'Sortino_Ratio':索提诺比, 'Winning_Rate':日胜率, 'Num_Days':交易天数
    'Calmar_Ratio':卡玛比}
    '''
    Drawdown_Metrics_Dict = max_drawdown(Networth_Series)
    Return_Metrics_Dict   = return_metrics(Networth_Series, Trading_Days_Per_Year = Trading_Days_Per_Year)
    Drawdown_Metrics_Dict.update(Return_Metrics_Dict)
    Drawdown_Metrics_Dict['Calmar_Ratio'] = Drawdown_Metrics_Dict['Annualized_Return'] / abs(Drawdown_Metrics_Dict['Max_Drawdown'])
    return Drawdown_Metrics_Dict

# ----------------------------------------------------------------------
def get_key_metrics(Networth_Series, Trading_Days_Per_Year = 242):
    '''
    取得关键评价指标
    :param Networth_Series: pd.Series, 净值序列
    :param Trading_Days_Per_Year:
    :return: pd.DataFrame
    '''
    return pd.DataFrame({Key: Value for Key, Value in calc_metrics(Networth_Series, Trading_Days_Per_Year).iteritems() if Key in ['Annualized_Return', 'Annualized_Std', 'Max_Drawdown', 'Sharpe_Ratio', 'Calmar_Ratio']}, index=[0])


# def portfolio_counter(Portfolio_Frame, Plot = False, Save_Dir = None):
#     Portfolio_Frame['Year'] = Portfolio_Frame.index.year
#     Grouped_By_Year         = Portfolio_Frame.groupby('Year')
#     Extracted_Series        = pd.Series()
#     for Yearly_Groups in Grouped_By_Year:
#         Extracted_Series[str(Yearly_Groups[0])] = Yearly_Groups[1].drop('Year', 1).iloc[-1,:].sum()
#
#     if Plot is True:
#         Ax         = plt.figure().add_subplot(111)
#         Plot_Title = 'Number of Trading Varieties'
#         Ax.set_title(Plot_Title)
#         Extracted_Series.plot(ax = Ax)
#
#     if Save_Dir is not None:
#         if not os.path.exists(Save_Dir):
#             os.makedirs(Save_Dir)
#         if Plot is True:
#             plt.savefig(Save_Dir + '/' + Plot_Title + '.png')
#     plt.close('all')
#     return Extracted_Series

# ----------------------------------------------------------------------
def yearly_metrics(Networth_Series, Trading_Days_Per_Year = 242, Key_List = ['Annualized_Return', 'Annualized_Std', 'Max_Drawdown', 'Sharpe_Ratio', 'Sortino_Ratio', 'Calmar_Ratio', 'Winning_Rate', 'Num_Days'], Save_Dir = None):
    '''
    年度指标计算
    :param Networth_Series: pd.Series, 净值序列
    :param Trading_Days_Per_Year:
    :param Key_List: list, 需要的指标
    :param Save_Dir: str, 保存路径
    :return: pd.DataFrame, index = 年份, columns = 指标
    '''
    Networth_Series     = Networth_Series.dropna()
    Networth_Date_Frame = pd.DataFrame({'Networth':Networth_Series.values, 'Year':Networth_Series.index.year}, index = Networth_Series.index)
    Grouped_By_Year     = Networth_Date_Frame.groupby('Year')
    Yearly_Metric_Dict  = {}
    for Yearly_Groups in Grouped_By_Year:
        Yearly_Metric_Dict[str(Yearly_Groups[0])] = calc_metrics(Yearly_Groups[1].Networth, Trading_Days_Per_Year = Trading_Days_Per_Year)

    Metrics_Frame                  = pd.DataFrame(Yearly_Metric_Dict).loc[Key_List,:].transpose()
    Metrics_Frame.loc['Average',:] = Metrics_Frame.mean(axis = 0)

    if Save_Dir is not None:
        if not os.path.exists(Save_Dir):
            os.makedirs(Save_Dir)
        Csv_Name = Save_Dir+'/Metrics.csv'
        with open(Csv_Name, 'a') as Csvfile:
            Book_Writer = csv.writer(Csvfile, delimiter=',')
            Book_Writer.writerow(['\n'])
            Book_Writer.writerow(['Yearly Metrics'])
        Metrics_Frame.to_csv(Csv_Name, mode = 'a')

    return Metrics_Frame

# ----------------------------------------------------------------------
def monthly_return_analysis(Networth_Series, Save_Dir = None):
    '''
    月度指标计算
    :param Networth_Series: pd.Series, 净值序列
    :param Save_Dir: str, 保存路径
    :return: pd.DataFrame, index = 年份, columns = 月份 + 月内日胜率 + 月度年化收益
    '''
    Networth_Series     = Networth_Series.dropna()
    Networth_Date_Frame = pd.DataFrame({'Networth':Networth_Series.values, 'Year':Networth_Series.index.year, 'Month':Networth_Series.index.month}, index = Networth_Series.index)
    Grouped_By_Year     = Networth_Date_Frame.groupby('Year')
    Row_List            = []
    for Yearly_Groups in Grouped_By_Year:
        Curr_Year        = str(Yearly_Groups[0])
        Return_Series    = pd.Series(name = Curr_Year)
        Grouped_By_Month = Yearly_Groups[1].groupby('Month')
        for Monthly_Groups in Grouped_By_Month:
            Curr_Month                = str(Monthly_Groups[0])
            Return_Series[Curr_Month] = Monthly_Groups[1].Networth[-1] / Monthly_Groups[1].Networth[0] - 1

        Return_Series['Winning Rate']  = Return_Series[Return_Series > 0].size / Return_Series.dropna().size
        Return_Series['Annual Return'] = Yearly_Groups[1].Networth[-1] / Yearly_Groups[1].Networth[0] - 1
        Row_List.append(Return_Series)

    Return_Analysis_Frame                  = pd.concat(Row_List, axis = 1).transpose()
    Return_Analysis_Frame.loc['Average',:] = Return_Analysis_Frame.mean(axis = 0)

    if Save_Dir is not None:
        if not os.path.exists(Save_Dir):
            os.makedirs(Save_Dir)
        Csv_Name = Save_Dir+'/Metrics.csv'
        with open(Csv_Name, 'a') as Csvfile:
            Book_Writer = csv.writer(Csvfile, delimiter=',')
            Book_Writer.writerow(['\n'])
            Book_Writer.writerow(['Return Analysis'])
        Return_Analysis_Frame.to_csv(Csv_Name, mode = 'a')

    return Return_Analysis_Frame

# ----------------------------------------------------------------------
def networth_extract(Networth_Series, Period = 'M', Plot = False, Save_Dir = None):
    '''
    提取月度周度净值
    :param Networth_Series: 净值序列
    :param Period: 需要的周期
    :param Plot: 是否绘图
    :param Save_Dir: 保存路径
    :return:
    '''
    Networth_Series     = Networth_Series.dropna()
    Extracted_Series    = pd.Series()

    if Period == 'M':
        Networth_Date_Frame = pd.DataFrame({'Networth':Networth_Series.values, 'Year':Networth_Series.index.year, 'Month':Networth_Series.index.month}, index = Networth_Series.index)
        Grouped_By_Year     = Networth_Date_Frame.groupby('Year')
        for Yearly_Groups in Grouped_By_Year:
            Curr_Year        = str(Yearly_Groups[0])
            Grouped_By_Month = Yearly_Groups[1].groupby('Month')
            for Monthly_Groups in Grouped_By_Month:
                Curr_Month = str(Monthly_Groups[0])
                Curr_Data  = Monthly_Groups[1].Networth
                Extracted_Series[Curr_Year+'/'+Curr_Month] = Curr_Data[-1]

    elif Period == 'W':
        Networth_Date_Frame = pd.DataFrame({'Networth':Networth_Series.values, 'Weekday':Networth_Series.index.weekday}, index = Networth_Series.index)
        Extracted_Series = Networth_Date_Frame.groupby('Weekday').get_group(4).Networth

    Extracted_Series = Extracted_Series / Extracted_Series[0]

    Plot_Title = 'Networth_Period-' + Period
    if Plot is True:
        Ax = plt.figure().add_subplot(111)
        Ax.set_title(Plot_Title)
        Extracted_Series.plot(ax = Ax)

    if Save_Dir is not None:
        if not os.path.exists(Save_Dir):
            os.makedirs(Save_Dir)
        if Plot is True:
            plt.savefig(Save_Dir + '/' + Plot_Title + '.png')

    plt.close('all')
    return Extracted_Series

#
# def open_interest_proportion(Oi_Frame, Net_Shares_Frame, Plot = False, Plot_Loc = ['Mean','2013'], Save_Dir = None):
#     Oi_Frame                                = Oi_Frame.loc[Net_Shares_Frame.index, Net_Shares_Frame.columns]
#     Net_Shares_Frame[Net_Shares_Frame == 0] = np.nan
#     Oi_Frame[Oi_Frame == 0]                 = np.nan
#     Divided_Frame                           = Net_Shares_Frame / Oi_Frame
#     Divided_Frame['Year']                   = Divided_Frame.index.year
#     Grouped_By_Year                         = Divided_Frame.groupby('Year')
#     Temp_Dict                               = {}
#
#     for Yearly_Group in Grouped_By_Year:
#         Record_Frame = pd.DataFrame(index = ['Max', 'Mean', 'Size'], columns = Net_Shares_Frame.columns)
#         Curr_Slice   = Yearly_Group[1].drop('Year',1)
#         Record_Frame.loc['Max',:]  = Curr_Slice.max()
#         Record_Frame.loc['Mean',:] = Curr_Slice.mean()
#         for Col_Name in Curr_Slice:
#             Curr_Size = Curr_Slice[Col_Name].dropna().size
#             if Curr_Size == 0:
#                 Curr_Size = np.nan
#             Record_Frame.loc['Size',Col_Name] = Curr_Size
#
#         Temp_Dict[str(Yearly_Group[0])] = Record_Frame
#
#     Proportion_Panel = pd.Panel(Temp_Dict).swapaxes('items','major_axis')
#
#     if Plot is True:
#         Ax           = plt.figure().add_subplot(111)
#         Plot_Title   = 'Oi_Proportion_'+Plot_Loc[0]+'_'+Plot_Loc[1]
#         Ax.set_title(Plot_Title)
#         Temp_Data    = Proportion_Panel[Plot_Loc[0],Plot_Loc[1],:]
#         Data_To_Plot = Temp_Data[Temp_Data < 1]
#         Dropped_Col  = [x for x in Temp_Data.index if x not in Data_To_Plot.index]
#         if Dropped_Col:
#             print 'Warning: Columns: ' + repr(Dropped_Col) + ' have values > 1, excluded from plot. ( backtest.py:open_interest_proportion() )'
#         try:
#             Data_To_Plot.transpose().plot(ax = Ax, kind = 'bar')
#         except:
#             pass
#
#     Proportion_Panel.loc[:,'Average',:] = Proportion_Panel.mean()
#
#     if Save_Dir is not None:
#         if not os.path.exists(Save_Dir):
#             os.makedirs(Save_Dir)
#         if Plot is True:
#             plt.savefig(Save_Dir + '/' + Plot_Title + '.png')
#         Csv_Name = Save_Dir+'/Metrics.csv'
#         for Item in Proportion_Panel:
#             with open(Csv_Name, 'a') as Csvfile:
#                 Book_Writer = csv.writer(Csvfile, delimiter=',')
#                 Book_Writer.writerow(['\n'])
#                 Book_Writer.writerow(['Oi Proportion-'+Item])
#             Proportion_Panel[Item].to_csv(Csv_Name, mode = 'a')
#     plt.close('all')
#     return Proportion_Panel


# def strategy_turnover(Price_Frame, Multiplier_Series, Net_Delta_Shares_Frame, Net_Shares_Frame, Period = 'D', Plot = False, Save_Dir = None):
#     Price_Frame                                         = Price_Frame.loc[Net_Delta_Shares_Frame.index, Net_Delta_Shares_Frame.columns]
#     Multiplier_Series                                   = Multiplier_Series[Net_Delta_Shares_Frame.columns]
#     Net_Delta_Shares_Frame[Net_Delta_Shares_Frame == 0] = np.nan
#     Net_Shares_Frame[Net_Shares_Frame == 0]             = np.nan
#     Price_Frame[Price_Frame == 0]                       = np.nan
#     Delta_Money_Series                                  = (Price_Frame * Net_Delta_Shares_Frame * Multiplier_Series).sum(axis = 1)
#     Money_Series                                        = (Price_Frame * Net_Shares_Frame * Multiplier_Series).sum(axis = 1)
#     Turnover_Series                                     = Delta_Money_Series / Money_Series
#
#     if Period == 'M':
#         Turnover_Series = Turnover_Series.dropna()
#         Temp_Frame      = pd.DataFrame({'Turnover':Turnover_Series, 'Year':Turnover_Series.index.year, 'Month':Turnover_Series.index.month})
#         Grouped_By_Year = Temp_Frame.groupby('Year')
#         Turnover_Series = pd.Series()
#         for Yearly_Groups in Grouped_By_Year:
#             Curr_Year        = str(Yearly_Groups[0])
#             Grouped_By_Month = Yearly_Groups[1].groupby('Month')
#             for Monthly_Groups in Grouped_By_Month:
#                 Curr_Month = str(Monthly_Groups[0])
#                 Curr_Data  = Monthly_Groups[1].Turnover
#                 Turnover_Series[Curr_Year+'/'+Curr_Month] = Curr_Data.mean()
#
#     if Plot is True:
#         Ax         = plt.figure().add_subplot(111)
#         Plot_Title = 'Turnover_Period-'+Period
#         Ax.set_title(Plot_Title)
#         try:
#             Turnover_Series.plot(ax = Ax)
#         except:
#             pass
#
#     if Save_Dir is not None:
#         if not os.path.exists(Save_Dir):
#             os.makedirs(Save_Dir)
#         if Plot is True:
#             plt.savefig(Save_Dir + '/' + Plot_Title + '.png')
#     plt.close('all')
#     return Turnover_Series



