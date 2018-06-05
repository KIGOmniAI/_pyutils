# -*- coding: utf-8 -*-
"""
Created on Fri May 26 17:45:41 2017

@author: Andrei
"""


__author__     = "Andrei Ionut DAMIAN"
__project__    = "GoDriveCarBox"
__copyright__  = "Copyright 2007 4E Software"
__credits__    = ["Andrei Simion"]
__license__    = "GPL"
__version__    = "0.1.1"
__maintainer__ = "Andrei Ionut DAMIAN"
__email__      = "damian@4esoft.ro"
__status__     = "Production"
__library__    = "DATA EXPLORER"
__created__    = "2017-01-25"
__modified__   = "2017-05-25"
__lib__        = "GDCBDE"

import matplotlib.pyplot as plt

from gdcb_azure_helper import MSSQLHelper
import pandas as pd
from datetime import datetime as dt
import numpy as np
import json

import time as tm


def clean_nonascii_df(df):
  for col in df.columns:
    if df[col].dtype=='O':
      df[col] = df[col].astype(str)
      df[col] = df[col].apply(
        lambda x: ''.join([" " if ord(i) < 32 or ord(i) > 126 else i  
                           for i in x]))
  return df
  
class RandomWalker:
  
  def __init__(self,range_min, range_max, int_vals = False):
    self.step = 0
    avg = (range_max - range_min) / 2
    self.value = range_min + np.random.uniform(low=0.0, high=avg)
    self.inc_max = 2
    if avg<1:
      self.inc_max = np.random.uniform(low=0.01, high = avg/2)      
    self.min = range_min
    self.max = range_max
    self.int_vals = int_vals
    if self.int_vals:
      self.value = int(self.value)
    return
  
  def GetValue(self):
    self.step += 1
    if self.int_vals:
      increment = np.random.randint(-self.inc_max,self.inc_max) 
    else:
      increment = np.random.uniform(low = -self.inc_max, 
                                    high = self.inc_max)
    self.value += increment
    if self.value < self.min:
      self.value = self.min
      
    if self.value >self.max:
      self.value = self.max
      
    return self.value

class GDCBExplorer:
  """
  GDCB Data Explorer main class
   - uploads data to Azure via GDCB Azure Helper engine
   - downloads data for model training and prediction
   - acts as a general data broker
  """
  def __init__(self, parent_log = None):
    assert parent_log != None
    assert parent_log.config_data != None
    
    self.parent_log = parent_log
    self.config_data = self.parent_log.config_data
    self.FULL_DEBUG = True
    pd.options.display.float_format = '{:,.3f}'.format
    pd.set_option('expand_frame_repr', False)
    np.set_printoptions(precision = 3, suppress = True)
    self.sql_eng = MSSQLHelper(parent_log = self)
    
    self.MODULE = "{} v{}".format(__library__,__version__)
    self._logger("INIT "+self.MODULE)

    if self.FULL_DEBUG:
        self._logger(self.s_prefix)
        self._logger("__name__: {}".format(__name__))
        self._logger("__file__: {}".format(__file__))
    self._load_config()
    
    self.SetupVariables()
    return

  def _load_config(self, str_file = 'gdcb_config.txt'):
    """
    Load JSON configuration file
    """
    cfg_file = open(str_file)
    self.config_data = json.load(cfg_file) 
    return
  
  def SetupVariables(self):
    """
     load predictor variables from SQL Server repository and prepare raw-data 
     dataframe structure (by loading)
    """
    self._logger("Setup predictors and raw data repo...")
    s_pred_table = self.config_data["PREDICTOR_TABLE"]
    s_rawd_table = self.config_data["RAWDATA_TABLE"]
    s_cars_table = self.config_data["CARS_TABLE"]
    
    self.code_field = self.config_data["CODE_FIELD"]
    self.size_field = self.config_data["SIZE_FIELD"]
    
    self.min_field = self.config_data["MIN_FIELD"]
    self.max_field = self.config_data["MAX_FIELD"]
    self.mul_field = self.config_data["MUL_FIELD"]  
    self.add_field = self.config_data["ADD_FIELD"] 
    self.units_field = self.config_data["UNITS_FIELD"]  
    self.active_field = self.config_data["ENABLED_FIELD"]
    
    self.raw_nval_field = self.config_data["RAW_NVAL_FIELD"]
    self.raw_sval_field = self.config_data["RAW_SVAL_FIELD"]
    self.raw_code_field = self.config_data["RAW_CODE_FIELD"]
    self.raw_time_field = self.config_data["RAW_TIME_FIELD"]
    self.raw_cari_field = self.config_data["RAW_CARI_FIELD"]
    
    self.raw_vwnv_field = self.config_data["RAW_VIEWABLE_VAL_FIELD"]
    self.raw_vwsv_field = self.config_data["RAW_VIEWABLE_STR_FIELD"]
    
    self.df_predictors = self.sql_eng.ReadTable(s_pred_table)
    if not self.df_predictors is None:
      self.df_predictors.fillna(0,inplace = True)
      self._logger("Loaded {} predictors".format(self.df_predictors.shape[0]))
      
    self.df_rawdata = self.sql_eng.GetEmptyTable(s_rawd_table)
    if not self.df_rawdata is None:
      self.df_rawdata.drop(self.config_data["RAW_IGNR_FIELD"], 
                           axis=1, inplace=True)
      self._logger("RawData: {}".format(list(self.df_rawdata.columns)))

    self.df_cars = self.sql_eng.ReadTable(s_cars_table)
    if not self.df_cars is None:
      self._logger("Loaded {} cars".format(self.df_cars.shape[0]))

    self._logger("Done data preparation.")
    return
  
  def DumpRawData(self):
    """
    saves raw data to the sql table
    """
    assert not (self.sql_eng.engine is None)
    self._logger("Saving raw data ...")
    self.sql_eng.SaveTable(self.df_rawdata, self.config_data["RAWDATA_TABLE"])
    self._logger("Done saving raw data.")
    return
  
  def EmptyRawData(self):
    self.df_rawdata = self.df_rawdata[0:0]
    return
    
  def _sample_number(self,nbytes):    
    v = 0
    for i in range(nbytes*2):
      v += np.random.randint(0,16) * (16**i)
    return v
    
  def SampleRaw(self, sample_size, car_id):
    car_components = {}
    self._logger("Sampling data [{}]...".format(sample_size))
    nr_codes = self.df_predictors.shape[0]
    self.EmptyRawData()
    assert nr_codes != 0
    for i in range(sample_size):
      n = np.random.randint(0,nr_codes)
      while not (self.df_predictors.loc[n,self.active_field]):
        n = np.random.randint(0, nr_codes)

      s_code = self.df_predictors.loc[n,self.code_field]       
      min_val = float(self.df_predictors.loc[n,self.min_field])
      max_val = float(self.df_predictors.loc[n,self.max_field])  
      mul_val = float(self.df_predictors.loc[n,self.mul_field]) 
      add_val = float(self.df_predictors.loc[n,self.add_field]) 
      sunits =  self.df_predictors.loc[n,self.units_field] 
      
      if not (s_code in car_components.keys()):
        car_components[s_code] = RandomWalker(min_val, max_val)
        
      val = car_components[s_code].GetValue()
      
      nval = int((val - add_val) / mul_val)
      #nval = (val - add_val) / mul_val

      nowtime = dt.now()
      strnowtime = nowtime.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
      sb16val = hex(nval)
      #sb16val = hex(int(nval))

      self.df_rawdata.loc[i,self.raw_code_field] = str(s_code)
      self.df_rawdata.loc[i,self.raw_nval_field] = nval
      self.df_rawdata.loc[i,self.raw_sval_field] = sb16val
      self.df_rawdata.loc[i,self.raw_time_field] = strnowtime
      self.df_rawdata.loc[i,self.raw_cari_field] = str(car_id)
      self.df_rawdata.loc[i,self.raw_vwnv_field] = str(val)
      self.df_rawdata.loc[i,self.raw_vwsv_field] = "{:.2f} {}".format(
                                    val,
                                    sunits)

    self.df_rawdata[self.raw_code_field] = self.df_rawdata[self.raw_code_field].astype(int) 
    self._logger("Done sampling data.")
    self.DumpRawData()
    
      
  def SampleRange(self, nr_samples, sample_size):
    self._logger("Sampling {} data of size [{}]...".format(
                 nr_samples, 
                 sample_size))
    t0 = tm.time()
    for i in range(nr_samples):
      c = np.random.randint(0, self.df_cars.shape[0])
      carid = self.df_cars.iloc[c,0] 
      self._logger("Sampling {}/{} for car:{}".format(i,nr_samples,carid))
      self.SampleRaw(sample_size = sample_size, car_id = carid)
    t1 = tm.time()
    self._logger("Data sampling for {} data of size [{}] finished in {:.1f}s".format(
                   nr_samples, sample_size, t1-t0))
    return
    
  def TelemetryStatistics(self):
    
    s_tab = self.config_data["VIEW_ALLDATA"]
    s_desc = self.config_data["RAW_CODE_DESCR"]
    df = self.sql_eng.ReadTable(s_tab)
    codes = list(self.df_predictors[self.code_field])
    assert len(codes) != 0
    for code in codes:
      df_temp = df[df[self.raw_code_field] == code]
      if df_temp.shape[0] >0:
        df_temp.reset_index(drop = True, inplace = True)
        slabel = df_temp.loc[0,s_desc]
        if slabel[:3] != "PID":      
          self._logger("Generating statistics [code: {} recs:{} desc:{}".format(
                       code,
                       df_temp.shape[0],
                       slabel[:15]))
          values = df_temp[self.raw_nval_field]
          plt.figure()
          plt.hist(values)
          plt.title(slabel)
          sfile = self.img_file_base + str(code)+".png"
          plt.savefig(sfile)
    return
    
  def CleanupCache(self):
    self.sql_eng.ClearCache()
    return
    
  def _logger(self, logstr, show = True):
    logstr = "[{}] ".format(__lib__) + logstr
    self.parent_log._logger(logstr, show)

    return


if __name__ =="__main__":
  
  RUN_UPLOAD = False 
  
  if RUN_UPLOAD:
    explorer = GDCBExplorer()
    explorer.CleanupCache()
    dft = pd.read_csv("../tests/mode01_codes_raw.csv",encoding = "ISO-8859-1") 
    dft = clean_nonascii_df(dft)
    dft.to_csv("../tests/mode01_codes.csv", index=False)
    df = pd.read_csv("../tests/mode01_codes.csv") 
    
    explorer.sql_eng.OverwriteTable(df,"Codes")
    print("\n\n Restarting ...\n\n")
    del explorer
  
  explorer = GDCBExplorer()    
  explorer.SampleRange(1,120)
  #explorer.TelemetryStatistics()
  
    
  