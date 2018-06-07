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


###############################

import sys,os
import termios
import tty

from contextlib import contextmanager
import signal


""" Allow single-key keyboard input (no <enter> needed) 
     Sample Usage:
        with interactive.raw_read() as raw:
          while not exit_flag:
            keypress = raw.getch()
            exit_flag = handle_key(keypress)
"""



@contextmanager
def raw_read():
    """Enable Non-blocking single character read"""

    class RawReadCtrlChar(Exception): pass
    class  RawReader():
        def getch(self):
            termios.tcflush(sys.stdin, termios.TCIOFLUSH)
            c = sys.stdin.read(1)
            if c in ("\03", "\04", "\1a"): # break on ctrl-c, d or z
                raise RawReadCtrlChar(c) 
            return c

    fd = sys.stdin.fileno()
    old_settings = termios.tcgetattr(fd)
    try:
        tty.setraw(fd)
        yield RawReader()
    except RawReadCtrlChar as ex:
        if str(ex) == "\1a": raise signal.SIGSTP
    finally:
        termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)