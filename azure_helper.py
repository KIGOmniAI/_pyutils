# -*- coding: utf-8 -*-
"""
Created on Wed Jan 25 19:43:09 2017

@history:
  2017-11-13 MSSQLHelper can be initialized if only a parent_log is provided; now it uses config_data from parent_log
"""

from __future__ import print_function
import pandas as pd
import pyodbc
import urllib 
from sqlalchemy import create_engine
import time as tm
import os
    
__author__     = "Andrei Ionut DAMIAN"
__copyright__  = "Copyright 2007 4E Software"
__credits__    = ["Andrei Simion"]
__license__    = "GPL"
__version__    = "1.3.3"
__maintainer__ = "Andrei Ionut DAMIAN"
__email__      = "damian@4esoft.ro"
__status__     = "Production"
__library__    = "AZURE SQL HELPER"
__created__    = "2017-01-25"
__modified__   = "2017-06-01"
__lib__        = "SQLHLP"


def start_timer():    
  return tm.time()

def end_timer(start_timer):
  return(tm.time()-start_timer)

def print_progress(str_text):
  print("\r"+str_text, end='\r', flush=True)
  return

class MSSQLHelper:
  """
   MS SQL Helper engine
   REQUIRES Logger object methods/props
  """
  def __init__(self, parent_log = None): 
    assert parent_log != None
    assert parent_log.config_data != None

    self.DEBUG = 1
    self.debug_str_size = 35

    self.parent_log = parent_log
    self.MODULE = '[{} v{}]'.format(__library__,__version__)

    config_data = self.parent_log.config_data

    self.driver   = config_data["DRIVER"]
    self.server   = config_data["SERVER" ]
    self.database = config_data["DATABASE"]
    self.username = config_data["USERNAME"]
    self.password = config_data["PASSWORD"]


    self.dfolder  = self.parent_log._base_folder        
    self.data_folder = self.dfolder
    self.dfolder = os.path.join(self.dfolder, "db_cache")
    
    if not os.path.isdir(self.dfolder):
        self._logger("Creating data folder:{}".format(
                            self.dfolder[-self.debug_str_size:]))
        os.makedirs(self.dfolder)
    else:
        self._logger("Using data folder:...{}".format(
                self.dfolder[-self.debug_str_size:]))
        
    self.connstr = 'DRIVER=' + self.driver
    self.connstr+= ';SERVER=' + self.server
    self.connstr+= ';DATABASE=' + self.database
    self.connstr+= ';UID=' + self.username
    self.connstr+= ';PWD=' + self.password
    self.engine = None

    sql_params = urllib.parse.quote_plus(self.connstr)

    try:
        self._logger("ODBC Conn: {}...".format(self.connstr[:self.debug_str_size]))
        self.conn = pyodbc.connect(self.connstr, 
                                   timeout = 2)   
        self.engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % sql_params,
                                    connect_args={'connect_timeout': 2})
        self._logger("Connection created on "+self.server)
    except Exception as err: #pyodbc.Error as err:
        self._logger("FAILED ODBC Conn!")
        self.HandleError(err)        
    return

  
  def Select(self,str_select, caching = True, convert_ascii = None):
      df = None
      try:
          str_fn = "".join(["_" if x in " ,;()*\\\\/[].><" else x for x in str_select])
          str_fn = str_fn.replace("__","_").replace("__","_")
          str_fn += ".csv"
          str_fn = os.path.join(self.dfolder,str_fn)
          if self.DEBUG > 1:
              self._logger("Using datafile: {}".format(str_fn))
          t0 = tm.time()
          if (not os.path.isfile(str_fn)) or (not caching):
              fmt_sql = " ".join(str_select.split())[:80]
              if self.DEBUG > 0:
                  self._logger("Download [{}..]".format(fmt_sql[:45]))
              else:
                  self._logger("Downloading data...")
              df = pd.read_sql(str_select, self.conn)
              if convert_ascii != None:
                  # now convert columns to ascii
                  for col in convert_ascii:
                      df[col] = df[col].apply(lambda x: ''.join(
                              [" " if ord(i) < 32 or ord(i) > 126 else i 
                                   for i in x]))
              if caching:
                  if self.DEBUG > 0:
                      self._logger("Saving to [..{}]...".format(str_fn[-self.debug_str_size:]))
                  else:
                      self._logger("Saving cache...")
                  df.to_csv(str_fn, index = False)                    
          else:
              if self.DEBUG > 0:
                  self._logger("Loading file [..{}] ...".format(str_fn[-self.debug_str_size:]))
              else:
                  self._logger("Loading file ...")
              df = pd.read_csv(str_fn)
          nsize = self.GetSize(df) / float(1024*1024)
          t1 = tm.time()
          tsec = t1-t0
          tmin = float(tsec) / 60
          self._logger("Dataset loaded: {:.2f}MB in {:.1f}s({:.1f}m) {} rows".format(
                       nsize,
                       tsec,
                       tmin,
                       df.shape[0],
                       str_select))
          if self.DEBUG>1:
              self._logger("Dataset head(3):\n{}".format(df.head(2)))
          #self._logger("  READ TABLE time: {:.1f}s ({:.2f}min)".format(tsec,tmin))
      except Exception as err: #pyodbc.Error as err:
          self.HandleError(err)
      return df
  
  
  def ReadTable(self, str_table):
    str_select = "SELECT * FROM ["+str_table+"]"
    return self.Select(str_select)
      
  def GetEmptyTable(self, str_table):
    str_select = "SELECT TOP (1) * FROM ["+str_table+"]"+" ORDER BY ID DESC"
    print(str_select)
    return self.Select(str_select)

      
  def ExecInsert(self, sInsertQuery):    
    _result = True        
    try:
        t0 = tm.time()
        cursor = self.conn
        cursor.execute(sInsertQuery)
        self.conn.commit()
        t1 = tm.time()
        tsec = t1-t0
        tmin = float(tsec) / 60
        self._logger("EXEC SQL  time: {:.1f}s ({:.2f}min)".format(tsec,tmin))
    except Exception as err: #pyodbc.Error as err:
        self.HandleError(err)                        
        _result = False
    return _result


  def SaveTable(self, df, sTable):
    dfsize = self.GetSize(df) / (1024*1024)
    try:      
        self._logger("SAVING TABLE [APPEND]({:,} records {:,.2f}MB)...".format(
                     df.shape[0],
                     dfsize))
        t0 = tm.time()
        df.to_sql(sTable, 
                  self.engine, 
                  index = False, 
                  if_exists = 'append')
        t1 = tm.time()
        tsec = t1-t0
        tmin = float(tsec) / 60
        self._logger("DONE SAVE TABLE. Time = {:.1f}s ({:.2f}min)".format(tsec,tmin))
    except Exception as err: #pyodbc.Error as err:
        self.HandleError(err)                    
    return

  def OverwriteTable(self, df, sTable):
    dfsize = self.GetSize(df) / (1024*1024)
    try:      
        self._logger("SAVING TABLE [OVERWRITE]({:,} records {:,.2f}MB)...".format(
                     df.shape[0],
                     dfsize))
        t0 = tm.time()
        df.to_sql(sTable, 
                  self.engine, 
                  index = False, 
                  if_exists = 'replace')
        t1 = tm.time()
        tsec = t1-t0
        tmin = float(tsec) / 60
        self._logger("DONE SAVE TABLE. Time = {:.1f}s ({:.2f}min)".format(tsec,tmin))
    except Exception as err: #pyodbc.Error as err:
        self.HandleError(err)                    
    return
      
  def Close(self):
    self.conn.close()
    return

  def HandleError(self, err):
      strerr = "ERROR: "+ str(err) #[:50]
      self._logger(strerr)
      return
  
  def GetSize(self,df):
      dfsize = df.values.nbytes + df.index.nbytes + df.columns.nbytes
      return dfsize
      
  
  def _logger(self, logstr, show = True):
      logstr = "[{}] ".format(__lib__) + logstr
      self.parent_log._logger(logstr, show)

      return

  def ClearCache(self):
    self._logger("Cleaning DB cache ...")
    self.EmptyFolder(self.dfolder)
    self._logger("Done cleaning DB cache.")
    return
    
  def EmptyFolder(self, sFolder):
    for the_file in os.listdir(sFolder):
        file_path = os.path.join(sFolder, the_file)
        try:
            if os.path.isfile(file_path):
                os.unlink(file_path)
            #elif os.path.isdir(file_path): shutil.rmtree(file_path)
        except Exception as e:
            print(e)    
    return
    

  
  def __exit__(self, exc_type, exc_val, exc_tb):
      self.conn.close()
      self._logger("__exit__")
      return
        
if __name__ == '__main__':

    print("ERROR: MSSQLHelper is library only!")
    
    
