# -*- coding: utf-8 -*-
"""
Created on Wed Jun  6 11:35:38 2018

@author: Andrei Ionut Damian
"""



class _Getch:
    """Gets a single character from standard input.  Does not echo to the
screen."""
    def __init__(self):
        try:
            self.impl = _GetchWindows()
        except ImportError:
            self.impl = _GetchUnix()

    def __call__(self): return self.impl()


class _GetchUnix:
    def __init__(self):
        import tty, sys
        return

    def __call__(self):
        import sys, tty, termios
        fd = sys.stdin.fileno()
        old_settings = termios.tcgetattr(fd)
        try:
            tty.setraw(sys.stdin.fileno())
            ch = sys.stdin.read(1)
        finally:
            termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)
        return ch


class _GetchWindows:
    def __init__(self):
        import msvcrt
        return

    def __call__(self):
        import msvcrt
        return msvcrt.getch()


getch = _Getch()


if __name__ == '__main__':
  
  import msvcrt
  def readch(echo=True):
    "Get a single character on Windows."
    while msvcrt.kbhit():  # clear out keyboard buffer
        msvcrt.getwch()
    ch = msvcrt.getwch()
    if ch in u'\x00\xe0':  # arrow or function key prefix?
        ch = msvcrt.getwch()  # second call returns the actual key code
    if echo:
        msvcrt.putwch(ch)
    return ch  
  
  while True:
    #if msvcrt.kbhit():
    #  ch = msvcrt.getwch()
      ch = readch()    
      print(ch)
