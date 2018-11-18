# encoding: UTF-8

"""
Barda
"""
import numpy as np
import pandas as pd
########################################################################
class Barda(object):
    def __init__(self, var, period ='M'):
        self.var = var
        self.period = period
        self.dat = pd.DataFrame()
        self.crtidx = 'xx'
        self.crtnum = 0
        self.crtbar = pd.Series()
        self.newsta = 0
        self.indexs = []
    # ----------------------------------------------------------------------
    def newbar(self, bar):
        for idtm in self.dat.index[self.crtnum:self.dat.index.size]:
            if bar.datetime > idtm:
                self.crtnum = min(self.crtnum+1, self.dat.index.size-1)
                self.crtidx = self.dat.index[self.crtnum]
                self.crtbar = pd.Series(index=self.dat.columns)
                self.newsta = 1
            else:
                if self.crtbar.count() == 0:
                    if bar.vtSymbol == self.var:
                        self.crtbar.name = idtm
                        for k in self.crtbar.index:
                            if k in bar.__dict__:
                                self.crtbar[k] = bar.__dict__[k]
                    break
                else:
                    self.newsta = 0
                    if bar.vtSymbol == self.var:
                        self.crtbar.name = idtm
                        try:
                            self.crtbar['high'] = max(self.crtbar['high'], bar.high)
                        except:
                            print 'err'
                        self.crtbar['low'] = min(self.crtbar['low'], bar.low)
                        self.crtbar['close'] = bar.close
                        self.crtbar['volume'] = self.crtbar['volume'] + bar.volume
                        self.crtbar['openInterest'] = bar.openInterest
                    break
        try:
            self.crtnum = self.indexs.index(self.crtidx)
        except:
            self.crtnum = 0
########################################################################