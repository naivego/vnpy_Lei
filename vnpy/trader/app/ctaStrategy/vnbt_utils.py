# encoding: UTF-8

import os

import csv
import copy
import re
import imp
import numpy  as np
import pandas as pd
import pickle as pk
from collections import OrderedDict as OD
from time import localtime, strftime
import importlib

# matplotlib.use('TkAgg')
from matplotlib.finance import candlestick2_ohlc
import matplotlib.ticker as ticker
import matplotlib.pyplot as plt
from ctaBase import *
from vnbt_metrics import calc_metrics



def plotsdk(stkdf, disfactors=None, Symbol='symbol', has2wind = False, Period =''):
    quotes= stkdf.loc[:]
    # quotes['mid'] = 0
    xdate = [itime for itime in quotes.index]
    mainwindfs = []
    subwindfs = ['ATR', 'grst', 'KD_k', 'KD_d']

    mfs = []
    sfs = []
    if has2wind:
        fig, (ax, ax1) = plt.subplots(2, sharex=True, figsize=(16, 7))
    else:
        fig, ax = plt.subplots(1, sharex=True, figsize=(16,6))

    #fig, ax = plt.subplots()
    fig.subplots_adjust(bottom=0.2, left=0.05)
    def mydate(x, pos):
        try:
            return xdate[int(x)]
        except IndexError:
            return ''
    candlestick2_ohlc(ax, quotes['open'], quotes['high'], quotes['low'], quotes['close'], width=0.5, colorup='r', colordown='green')
    ax.xaxis.set_major_locator(ticker.MaxNLocator(4))

    plt.title(Symbol + '_' + Period)
    plt.xlabel("time")
    plt.ylabel("price")

    ax.xaxis.set_major_formatter(ticker.FuncFormatter(mydate))
    # fig.autofmt_xdate()
    # print quotes.columns
    for disf in disfactors:
        if disf and disf in quotes.columns:
            if disf in mainwindfs:
                ax2 = ax.twinx()
                quotes[disf].plot(ax=ax2)
                ax2.xaxis.set_major_formatter(ticker.FuncFormatter(mydate))
                mfs.append(disf)
                continue

            if disf in subwindfs:
                pass
                # ax3 = ax1.twinx()
                quotes[disf].plot(ax=ax1)
                ax1.xaxis.set_major_formatter(ticker.FuncFormatter(mydate))
                sfs.append(disf)
            #
            else:
                quotes[disf].plot(ax=ax)
                ax.xaxis.set_major_formatter(ticker.FuncFormatter(mydate))
                mfs.append(disf)
    # quotes['mid'].plot(ax=ax1)

    fig.tight_layout()
    plt.grid()
    # plt.legend(disfactors)
    ax.legend(mfs)
    if has2wind:
        ax1.legend(sfs)
    plt.show()

    pass


# ----------------------------------------------------------------------
def sort_period_list(Period_List):
    return [SEQUENCE_PERIOD_DICT[x] for x in sorted([PERIOD_SEQUENCE_DICT[y] for y in Period_List])]

# ----------------------------------------------------------------------
def contract_num_to_str(Contract_Num, Variety_Key = '', Year_Str = None):
    Contract_Num_Str = str(int(Contract_Num))
    if len(Contract_Num_Str) == 3:
        if Year_Str:
            Contract_Num_Str = Year_Str[2] + Contract_Num_Str
        else:
            Contract_Num_Str = '0' + Contract_Num_Str
    elif len(Contract_Num_Str) == 2:
        if Year_Str:
            Contract_Num_Str = Year_Str[-2:] + Contract_Num_Str
        else:
            Contract_Num_Str = '00' + Contract_Num_Str
    elif len(Contract_Num_Str) == 1:
        if Year_Str:
            Contract_Num_Str = Year_Str[-3:] + Contract_Num_Str
        else:
            Contract_Num_Str = '000' + Contract_Num_Str
    return Variety_Key + Contract_Num_Str

# ----------------------------------------------------------------------
def import_all(Strategy_Dir):
    # 用来保存策略类的字典
    Strategy_Class_Dict = {}
    # 遍历strategy目录下的文件
    for root, subdirs, files in os.walk(Strategy_Dir):
        for name in files:
            # 只有文件名中包含strategy且非.pyc的文件，才是策略文件
            if 'strategy' in name and ('.pyc' not in name) and  ('.json' not in name):
                # 模块名称需要上前缀
                moduleName = 'strategy.' + name.replace('.py', '')
                # 使用importlib动态载入模块
                module = importlib.import_module(moduleName)
                # 遍历模块下的对象，只有名称中包含'Strategy'的才是策略类
                for k in dir(module):
                    if 'Strategy' in k:
                        v = module.__getattribute__(k)
                        Strategy_Class_Dict[k] = v
    return Strategy_Class_Dict

# ----------------------------------------------------------------------
def append_to_index_dict(Rt_Dir, Index_Dict, Start_Date, End_Date, Future_Variety_Market_Dict, Other_Variety_Market_Dict = None):
    Trade_Date_Dt           = pd.read_csv(Rt_Dir + '/Trade_Date.csv').iloc[:,0]
    Trade_Date_Dt           = Trade_Date_Dt[Trade_Date_Dt >= Start_Date]
    Trade_Date_Dt           = Trade_Date_Dt[Trade_Date_Dt < End_Date]
    Colname_List            = [Key + '.CONTRACT' for Key in Future_Variety_Market_Dict.iterkeys()]
    Dominant_Contract_Frame = pd.read_csv(Rt_Dir + '/Dominant_Contract.csv', index_col = 0).loc[:,Colname_List]
    for Date in Trade_Date_Dt:
        Fill_Dict = {}
        for Variety, Market in Future_Variety_Market_Dict.iteritems():
            if Market not in Fill_Dict:
                Fill_Dict[Market] = []
            try:
                Fill_Dict[Market].append( Dominant_Contract_Frame.loc[Date,Variety+'.CONTRACT'] )
            except:
                Fill_Dict[Market].append('AU1612')
            finally:
                break
        if Other_Variety_Market_Dict:
            for Variety, Market in Other_Variety_Market_Dict.iteritems():
                if Market not in Fill_Dict:
                    Fill_Dict[Market] = []
                Fill_Dict[Market].append(Variety)
        Index_Dict[Date.replace('-','_')] = Fill_Dict
    return Index_Dict

# ----------------------------------------------------------------------
def calendar_spread_index_dict(Rt_Dir, Index_Dict, Start_Date, End_Date, Future_Variety_Market_Dict):
    Trade_Date_Dt = pd.read_csv(Rt_Dir + '/Trade_Date.csv').iloc[:, 0]
    Trade_Date_Dt = Trade_Date_Dt[Trade_Date_Dt > Start_Date]
    Trade_Date_Dt = Trade_Date_Dt[Trade_Date_Dt < End_Date]
    Colname_List  = [Key + '.CONTRACT' for Key in Future_Variety_Market_Dict.iterkeys()]
    Dominant_Contract_Frame = pd.read_csv(Rt_Dir + '/Dominant_Contract.csv', index_col=0).loc[:, Colname_List]
    for Date in Trade_Date_Dt:
        Fill_Dict = {}
        # for Variety, Market in Future_Variety_Market_Dict.iteritems():
        #     if Market not in Fill_Dict:
        #         Fill_Dict[Market] = []
        #     try:
        #         Fill_Dict[Market].append(Dominant_Contract_Frame.loc[Date, Variety + '.CONTRACT'])
        #     except:
        #         break
        # Fill_Dict[Future_Variety_Market_Dict.values()[0]] = ['AG1606', 'AG1612']
        Fill_Dict[Future_Variety_Market_Dict.values()[0]] = ['RB1605', 'RB1610']
        Index_Dict[Date.replace('-', '_')] = Fill_Dict
    return Index_Dict

# ----------------------------------------------------------------------
def calendar_spreadlegs_index_dict(Rt_Dir, DB_Rt_Dir, Index_Dict, Start_Date, End_Date, Leg_One, Leg_Two):
    #Dom_Set = Setting['Dom_Set']  # 交易合约配置： 0-主力&次主力， 1-主力， 2-次主力
    Trade_Date_Dt           = pd.read_csv(Rt_Dir + '/vnpy/trader/CHN_Date.csv').iloc[:, 0]
    Trade_Date_Dt = Trade_Date_Dt[Trade_Date_Dt >= Start_Date]
    Trade_Date_Dt = Trade_Date_Dt[Trade_Date_Dt < End_Date]

    Future_Variety_List = []
    Dom_Set_List        = []
    TD_Variety_List     = []
    if 'TD' in Leg_One['Variety']:
        TD_Variety_List.append(Leg_One['Variety'])
    else:
        Future_Variety_List.append(Leg_One['Variety'])
        Dom_Set_List.append(Leg_One['Dom_Set'])

    if 'TD' in Leg_Two['Variety']:
        TD_Variety_List.append(Leg_Two['Variety'])
    else:
        Future_Variety_List.append(Leg_Two['Variety'])
        Dom_Set_List.append(Leg_Two['Dom_Set'])

    DomContract_Store = pd.HDFStore(DB_Rt_Dir + '/data/db/CHN_F/II_O/VarietyFctr/CHN_F_II_O_VarietyFctr_DomContract_Original_Inter_DB.h5', 'r')
    DomContract2_Store = pd.HDFStore(DB_Rt_Dir + '/data/db/CHN_F/II_O/VarietyFctr/CHN_F_II_O_VarietyFctr_DomContract2_Original_Inter_DB.h5', 'r')

    for Date in Trade_Date_Dt:
        Fill_Dict      = {}
        Fill_Dict['F'] = []
        for i in range(len(Future_Variety_List)):
            Variety_Key = Future_Variety_List[i]
            Dom_Set     = Dom_Set_List[i]
            if Dom_Set == 1:  # 添加主力合约
                Contract = DomContract_Store[Variety_Key + '/d'].at[Date.replace('-', '_'), 'Contract']
                if not  Contract:
                    continue
                Contract = str(int(Contract))
                if len(Contract)   == 2:
                    Contract ='00'+Contract
                elif len(Contract) == 3:
                    Contract ='0'+Contract
                Symbol = Variety_Key + Contract
                if Symbol not in Fill_Dict['F']:
                    Fill_Dict['F'].append(Symbol)

            elif Dom_Set ==2 :           # 添加次主力合约
                Contract = DomContract2_Store[Variety_Key + '/d'].at[Date.replace('-', '_'), 'Contract']
                if not  Contract:
                    continue
                Contract = str(int(Contract))
                if len(Contract)   == 2:
                    Contract ='00'+Contract
                elif len(Contract) == 3:
                    Contract ='0'+Contract
                Symbol = Variety_Key + Contract
                if Symbol not in Fill_Dict['F']:
                    Fill_Dict['F'].append(Symbol)
        Temp_TD_Contract_List = []
        for Variety_Name in TD_Variety_List:
            if Variety_Name not in Temp_TD_Contract_List:
                Temp_TD_Contract_List.append(Variety_Name)
        if Temp_TD_Contract_List:
            Fill_Dict['TD'] = Temp_TD_Contract_List

        Index_Dict[Date.replace('-', '_')] = Fill_Dict

    DomContract_Store.close()
    DomContract2_Store.close()
    return Index_Dict

# ----------------------------------------------------------------------
def calendar_multvm_dom_index_dict(Rt_Dir, Index_Dict, Setting):
    Trade_Date_Dt           = pd.read_csv(Rt_Dir + '/Trade_Date.csv').iloc[:, 0]
    Start_Date              = Setting['Start_Date']
    End_Date                = Setting['End_Date']
    Future_Variety_List     = Setting['VMlist_F']
    TD_Variety_List         = Setting['VMlist_TD']
    Trade_Date_Dt           = Trade_Date_Dt[Trade_Date_Dt > Start_Date]
    Trade_Date_Dt           = Trade_Date_Dt[Trade_Date_Dt < End_Date]
    Colname_List            = [Variety_Name + '.CONTRACT' for Variety_Name in Future_Variety_List]
    Dominant_Contract_Frame = pd.read_csv(Rt_Dir + '/Dominant_Contract.csv', index_col=0).loc[:, Colname_List]
    for Date in Trade_Date_Dt:
        if Date == '2016-07-04':
            pass
        Fill_Dict      = {}
        Fill_Dict['F'] = []
        for varm in Future_Variety_List:
            if varm not in Fill_Dict['F']:
                try:
                    Fill_Dict['F'].append(Dominant_Contract_Frame.loc[Date, varm + '.CONTRACT'])
                except:
                    break

        Temp_TD_Contract_List = []
        for Variety_Name in TD_Variety_List:
            if Variety_Name not in Temp_TD_Contract_List:
                Temp_TD_Contract_List.append(Variety_Name)
        if Temp_TD_Contract_List:
            Fill_Dict['TD'] = Temp_TD_Contract_List
        Index_Dict[Date.replace('-', '_')] = Fill_Dict
    return Index_Dict

# ----------------------------------------------------------------------
def calendar_multvm_index_dict(Rt_Dir, DB_Rt_Dir, Index_Dict, Setting):
    Dom_Set = Setting['Dom_Set']  # 交易合约配置： 0-主力&次主力， 1-主力， 2-次主力
    Trade_Date_Dt           = pd.read_csv(Rt_Dir + '/Trade_Date.csv').iloc[:, 0]
    Start_Date              = Setting['Start_Date']
    End_Date                = Setting['End_Date']
    Trade_Date_Dt = Trade_Date_Dt[Trade_Date_Dt >= Start_Date]
    Trade_Date_Dt = Trade_Date_Dt[Trade_Date_Dt < End_Date]

    Future_Variety_List     = Setting['VMlist_F']
    TD_Variety_List         = Setting['VMlist_TD']

    DomContract_Store = pd.HDFStore(DB_Rt_Dir + '/data/db/CHN_F/II_O/VarietyFctr/CHN_F_II_O_VarietyFctr_DomContract_Original_Inter_DB.h5', 'r')
    DomContract2_Store = pd.HDFStore(DB_Rt_Dir + '/data/db/CHN_F/II_O/VarietyFctr/CHN_F_II_O_VarietyFctr_DomContract2_Original_Inter_DB.h5', 'r')

    LastD_DomSymbol_Dict={}
    LastD_Dom2Symbol_Dict = {}

    for Date in Trade_Date_Dt:
        Fill_Dict      = {}
        Fill_Dict['F'] = []
        if Dom_Set < 2 :                           # 添加主力合约
            for Variety_Key in Future_Variety_List:
                Contract = DomContract_Store[Variety_Key + '/d'].at[Date.replace('-', '_'), 'Contract']
                if not  Contract:
                    continue
                Contract = str(int(Contract))
                if len(Contract)   == 2:
                    Contract ='00'+Contract
                elif len(Contract) == 3:
                    Contract ='0'+Contract
                Symbol = Variety_Key + Contract

                if LastD_DomSymbol_Dict.has_key(Variety_Key):
                    if len(LastD_DomSymbol_Dict[Variety_Key]) == 1:
                        if Symbol not in LastD_DomSymbol_Dict[Variety_Key]:
                            LastD_DomSymbol_Dict[Variety_Key].append(Symbol)
                    elif len(LastD_DomSymbol_Dict[Variety_Key]) > 1:
                        LastD_DomSymbol_Dict[Variety_Key]=[Symbol]
                else:
                    LastD_DomSymbol_Dict[Variety_Key] = [Symbol]
                for syb in  LastD_DomSymbol_Dict[Variety_Key]:
                    if syb not in Fill_Dict['F']:
                        Fill_Dict['F'].append(syb)

        if Dom_Set ==0 or  Dom_Set ==2 :           # 添加次主力合约
            for Variety_Key in Future_Variety_List:
                Contract = DomContract2_Store[Variety_Key + '/d'].at[Date.replace('-', '_'), 'Contract']
                if not  Contract:
                    continue
                Contract = str(int(Contract))
                if len(Contract)   == 2:
                    Contract ='00'+Contract
                elif len(Contract) == 3:
                    Contract ='0'+Contract
                Symbol = Variety_Key + Contract

                if LastD_Dom2Symbol_Dict.has_key(Variety_Key):
                    if len(LastD_Dom2Symbol_Dict[Variety_Key]) == 1:
                        if Symbol not in LastD_Dom2Symbol_Dict[Variety_Key]:
                            LastD_Dom2Symbol_Dict[Variety_Key].append(Symbol)
                    elif len(LastD_Dom2Symbol_Dict[Variety_Key]) > 1:
                        LastD_Dom2Symbol_Dict[Variety_Key] = [Symbol]
                else:
                    LastD_Dom2Symbol_Dict[Variety_Key] = [Symbol]
                for syb in LastD_Dom2Symbol_Dict[Variety_Key]:
                    if syb not in Fill_Dict['F']:
                        Fill_Dict['F'].append(syb)

        Temp_TD_Contract_List = []
        for Variety_Name in TD_Variety_List:
            if Variety_Name not in Temp_TD_Contract_List:
                Temp_TD_Contract_List.append(Variety_Name)
        if Temp_TD_Contract_List:
            Fill_Dict['TD'] = Temp_TD_Contract_List

        Index_Dict[Date.replace('-', '_')] = Fill_Dict

    DomContract_Store.close()
    DomContract2_Store.close()
    return Index_Dict

# ----------------------------------------------------------------------
def multvm_dom_index_dict(Setting, Index_Dict):
    Dom_Set                 = Setting['Dom_Set']  # 交易合约配置： 0-主力&次主力， 1-主力， 2-次主力
    Trade_Date_Dt           = pd.read_csv(Setting['Rt_Dir'] + '/Trade_Date.csv').iloc[:, 0]
    Start_Date              = Setting['Start_Date']
    End_Date                = Setting['End_Date']
    Trade_Date_Dt           = Trade_Date_Dt[Trade_Date_Dt >= Start_Date]
    Trade_Date_Dt           = Trade_Date_Dt[Trade_Date_Dt < End_Date]
    Future_Variety_List     = Setting['VMlist_F']

    sys.path.insert(0, Setting['DB_Rt_Dir'] + '/code/db_app.py')
    sys.path.insert(0, Setting['DB_Rt_Dir'] + '/code/')
    DB_App = getattr(imp.load_source('DB_App', Setting['DB_Rt_Dir'] + '/code/db_app.py'), 'DB_App')(Setting['DB_Rt_Dir'])

    Remove_List          = []
    Contract_Series_List = []
    for Variety_Key in Future_Variety_List:
        Dom_Df          = DB_App.load('DomContract', 'd', [Trade_Date_Dt.iat[0], Trade_Date_Dt.iat[-1]], Col_Param=['Contract'], Market='F', Variety_Key=Variety_Key)
        Contract_Series = pd.Series([contract_num_to_str(Tup[1][0], Variety_Key=Variety_Key, Year_Str=Tup[0].split('_')[0]) for Tup in Dom_Df.iterrows()], index = Dom_Df.index, name = Variety_Key)
        if Contract_Series.empty:
            Remove_List.append(Variety_Key)
        else:
            Contract_Series_List.append(Contract_Series)

    Setting['VMlist_F'] = list(set(Future_Variety_List) - set(Remove_List))
    Contract_Df         = pd.concat(Contract_Series_List,axis = 1)
    for Date in Trade_Date_Dt:
        Index_Dict[Date] = {'F':Contract_Df.loc[Date.replace('-','_'),:].dropna().tolist()}

    return Index_Dict

# ----------------------------------------------------------------------
def strip_dict(Temp_Dict):
    Temp_List = []
    [Temp_List.extend(x) for x in Temp_Dict.itervalues()]
    return Temp_List

# ----------------------------------------------------------------------
def combination_builder(Choice_Frame, Looping_Key_List):
    """
    Static function for building combinations of configuration for loop based
        on DataFrame input

        similar functionalities with itertools.product(lists)

    Parameters
    ----------
    Config_Frame : pandas.DataFrame
        A dataframe with column names as the config keys and column values
        as the options.
    Looping_Key_List : list of str
        A list of str that contains all the selected keys in the column
        names that will be iterated. For unselected column names, the first
        option from their columns will be assigned as values by default.

    Return
    ----------
    Temp_List : list
        A list of dicts that contains key-value config pairs.

    Test
    ----------
    a = pd.DataFrame([[1,2,3],[11,22]],columns = ['a','b','c'])
    Looping_Key_List = ['a','b']
    b = combination_builder(a, Looping_Key_List)
    """
    if len(Looping_Key_List) == 0:
        Temp_Dict = {}
        for Key_Ind in range(Choice_Frame.shape[1]):
            Curr_Value = Choice_Frame.iloc[0, Key_Ind]
            if Curr_Value != Curr_Value:
                Curr_Value = None
            Temp_Dict[Choice_Frame.columns.tolist()[Key_Ind]] = Curr_Value
        return [Temp_Dict]

    else:
        Curr_Key = Looping_Key_List[-1]
        New_Looping_Key_List = copy.deepcopy(Looping_Key_List)[:-1]
        Curr_Key_Series = Choice_Frame.loc[:, Curr_Key].dropna()
        New_Choice_Frame = Choice_Frame.copy()
        Temp_List = []
        for Curr_Value in Curr_Key_Series:
            New_Choice_Frame.loc[:, Curr_Key] = pd.Series([Curr_Value])
            Temp_List.extend(combination_builder(New_Choice_Frame, New_Looping_Key_List))
        return Temp_List

# ----------------------------------------------------------------------
def dict_combination(Curr_Dict, Value_Frame):
    Old_Col_Sequence = Curr_Dict.keys()
    Curr_Config_Df = pd.DataFrame(index=[0], columns=Curr_Dict.keys())
    for Key, Item in Curr_Dict.iteritems():
        Curr_Config_Df.set_value(0, Key, Item)
    for Key in Value_Frame.columns:
        del Curr_Config_Df[Key]
    Curr_Config_Df = pd.concat([Curr_Config_Df, Value_Frame], axis=1)
    return combination_builder(Curr_Config_Df[Old_Col_Sequence], Value_Frame.columns.tolist())

# ----------------------------------------------------------------------
def get_concated_combination_frame(Config_Dict, Recurring_Dict):
    Curr_Concated_Frame = pd.DataFrame()
    Curr_Value_Frame = pd.DataFrame()
    Key_List = Recurring_Dict.keys()
    for Key_Ind in range(len(Key_List)):
        Key = Key_List[Key_Ind]
        if type(Recurring_Dict[Key]) is not pd.core.frame.DataFrame:
            New_Value_Frame = get_concated_combination_frame(Config_Dict[Key], Recurring_Dict[Key])
        else:
            New_Value_Frame = Recurring_Dict[Key]

        New_Value_Frame.columns = [Key]
        Curr_Value_Frame = pd.concat([Curr_Value_Frame, New_Value_Frame], axis=1)
        if Key_Ind == len(Key_List) - 1:
            Combined_List = dict_combination(Config_Dict, Curr_Value_Frame)
            Curr_Concated_Frame = pd.DataFrame(index=range(len(Combined_List)), columns=[Key])
            for Ind in range(len(Combined_List)):
                Curr_Concated_Frame.set_value(Ind, Key, Combined_List[Ind])
    return Curr_Concated_Frame

# ----------------------------------------------------------------------
def distribute_config_folder_name(Config_List):
    for Ind in range(len(Config_List)):
        Config_List[Ind]['Serial_Num'] = str(Ind)
    return Config_List

# ----------------------------------------------------------------------
def config_list_builder(Config_Dict, Recurring_Dict):
    Concated_Frame = get_concated_combination_frame(Config_Dict, Recurring_Dict)
    Config_List    = distribute_config_folder_name([x[0] for x in Concated_Frame.values.tolist()])
    return Config_List

# ----------------------------------------------------------------------
def nested_dict_builder(Key_List, Value):
    '''
    Builds a nested dict with a list of key. The length of the Key_List
        represents the depth of the nested dict.

    Parameters
    ----------
    Key_List : list
        A list of str.

    Value : any
        Any python variable.

    Return
    ----------
    Curr_Dict : dict
        A nested dict with n levels of key and the final level's key is
        mapped to the value.

    Test
    ----------
    a = ['a', 'b']
    value = 1
    b = nested_dict_builder(a,value)
    '''
    Curr_Dict = {}
    if len(Key_List) > 1:
        Curr_Dict[Key_List[0]] = nested_dict_builder(Key_List[1:], Value)
    else:
        Curr_Dict[Key_List[-1]] = Value
    return Curr_Dict

# ----------------------------------------------------------------------
def save_config(Result_Dir, Config):
    with open(Result_Dir + '/config.csv', 'wb') as Csvfile:
        Row_Writer = csv.writer(Csvfile, delimiter=',')
        Row_Writer.writerow(['Strategy_Name', 'Start_Date', 'End_Date', 'Testing_Period'])
        Row_Writer.writerow([Config['Strategy_Name'], Config['Start_Date'], Config['End_Date'], Config['Testing_Period']])
        Row_Writer.writerow(['Strategy_Setting:'])
        for Key, Value in Config['Strategy_Setting'].iteritems():
            Row_Writer.writerow([Key, Value])

# ----------------------------------------------------------------------
def round_time(Date_Time, Period = 'M', Mod = 0):
    if Period == 'M':
        return Date_Time.replace(second = 0, microsecond = 0)
    elif Period == 'M5':
        return Date_Time.replace(minute = Date_Time.minute - Date_Time.minute % 5, second = 0, microsecond = 0)
    elif Period == 'M15':
        return Date_Time.replace(minute = Date_Time.minute - Date_Time.minute % 15, second = 0, microsecond = 0)
    elif Period == 'M30':
        return Date_Time.replace(minute = Date_Time.minute - Date_Time.minute % 30, second = 0, microsecond = 0)
    elif Period == 'H':
        return Date_Time.replace(minute = 0, second=0, microsecond=0)

# ----------------------------------------------------------------------
def get_variety_key_from_symbol(Symbol):
    if Symbol in NIGHT_MARKET_SG:
        return Symbol
    else:
        return re.search('[A-Za-z]*', Symbol).group()

# ----------------------------------------------------------------------
def revert_list_dict(List_Dict):
    New_List_Dict = {}
    for Key, Value_List in List_Dict.iteritems():
        for Value in Value_List:
            if Value not in New_List_Dict:
                New_List_Dict[Value] = [Key]
            else:
                New_List_Dict[Value].append(Key)
    return New_List_Dict

# ----------------------------------------------------------------------
def get_Variety_Period_Set(Temp_Dict):
    if Temp_Dict.has_key('Variety_Period_Dict'):
        Variety_Period_Dict = Temp_Dict['Variety_Period_Dict']
        Variety_List        = Variety_Period_Dict.keys()
        Period_List         = []

        VMlist_F            = [vm  for vm in Variety_List if 'TD' not in vm]
        VMlist_TD           = [vm for vm in Variety_List if 'TD' in vm]
        for k, v in Variety_Period_Dict.iteritems():
            for iv in v:
                if iv not in Period_List:
                    Period_List.append(iv)

        Temp_Dict['VMlist_F'] = VMlist_F
        Temp_Dict['VMlist_TD'] = VMlist_TD
        if not Temp_Dict.has_key('Barque_MinSize_Dict'):
            Temp_Dict['Barque_MinSize_Dict'] = {'T':0, 'M':0, 'M5':0, 'M15':0, 'M30':0, 'H':0, 'd':0}
            for Period in Period_List:
                Temp_Dict['Barque_MinSize_Dict'][Period] = 10

    else:
        VMlist_F  = []
        VMlist_TD = []

        if Temp_Dict.has_key('VMlist_F'):
            VMlist_F = Temp_Dict['VMlist_F']
        if Temp_Dict.has_key('VMlist_TD'):
            VMlist_TD = Temp_Dict['VMlist_TD']

        VMlist    = VMlist_F + VMlist_TD
        Period_List = []
        if Temp_Dict.has_key('Barque_MinSize_Dict'):
            Period_List = [ Period for Period, Size in Temp_Dict['Barque_MinSize_Dict'].iteritems() if Size > 0]

        Variety_Period_Dict = { vm: Period_List   for vm in  VMlist }
        Temp_Dict['Variety_Period_Dict'] = Variety_Period_Dict

# ----------------------------------------------------------------------
def set_period_size_dict(Setting_Dict, Variety_List, Period_Size_Dict = {}):
    Barque_MinSize_Dict = {"T": 0, "M": 0, "M5": 0, "M15": 0, "M30": 0, "H": 0, "d": 0}
    for Key, Value in Period_Size_Dict.iteritems():
        Barque_MinSize_Dict[Key] = Value
    Setting_Dict['Barque_MinSize_Dict'] = Barque_MinSize_Dict
    Variety_Period_Dict = {}
    for Variety in Variety_List:
        Variety_Period_Dict[Variety] = Period_Size_Dict.keys()
    Setting_Dict['Variety_Period_Dict'] = Variety_Period_Dict
    return Setting_Dict









