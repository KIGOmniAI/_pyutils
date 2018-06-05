import os
from datetime import datetime as dt

class Logger:
  def __init__(self, save_folder=os.path.join(os.getcwd(), 'temp'), lib=''):
    self.lib = lib
    self.s_prefix = dt.strftime(dt.now(), '%Y%m%d')
    self.s_prefix += "_"
    self.s_prefix += dt.strftime(dt.now(), '%H%M')
    self.s_prefix += "_"
    self.save_folder = save_folder
    self.log_file = os.path.join(self.save_folder,
                                 self.s_prefix + self.lib + "_log.txt")

    if not os.path.exists(self.save_folder):
      os.makedirs(self.save_folder)

  def _log(self, logstr, show=True):
    """
    log processing method
    """
    if not hasattr(self, 'log'):
      self.log = list()

    strnowtime = dt.now().strftime("[{}][%Y-%m-%d %H:%M:%S] ".format(self.lib))
    logstr = strnowtime + logstr
    self.log.append(logstr)
    if show:
      print(logstr, flush=True)
    try:
      log_output = open(self.log_file, 'w')
      for log_item in self.log:
        log_output.write("%s\n" % log_item)
      log_output.close()
    except:
      print(strnowtime + "Log write error !", flush=True)
    return