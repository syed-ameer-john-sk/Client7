"""
Logging script for Alstom AeroX Software projects. 

Written by Sebastien Postansque (479134)
v1.1
23/12/2021

"""

import logging
import os
import sys 
from datetime import datetime
import time 
from shutil import move

class Log():

    def __init__(self, log_name : str, file_path : str) -> None: 
        self.log_file_path = self._process_path(file_path, log_name)
        self.logging = self._createLogger(self.log_file_path, 
                "%(asctime)s [%(levelname)s] %(message)s", 
                "%m/%d/%Y %I:%M:%S %p",
                logging.DEBUG)
    
    def log_event(self, levels : str, msg):
        """ Log an event with the rigth level. """
        levels_list = levels.split(",")
        for level in levels_list:
            if level == "info":
                self.logging.info(msg)
            elif level == "warning":
                self.logging.warning(msg)
            elif level == "error":
                self.logging.error(msg)    
            elif level == "critical":
                self.logging.critical(msg)
            if level == "terminal":
                print(msg, file=sys.stdout)

    def set_log(self, std_type):
        """ Return a custom logger class for stdout and stderr. """
        if std_type == "stderr":
            return LoggerWriter(self.logging.error)
        return LoggerWriter(self.logging.info)

    @staticmethod
    def _process_path(file_path, log_name):
        """ Define the name and path of the log file. """
        logs_directory = os.path.abspath(file_path)
        if not os.path.exists(logs_directory):
            try:
                os.makedirs(logs_directory)
            except OSError as e:
                raise OSError('An error as been raised for logging: %s' % e)
        log_name += "_" + datetime.now().strftime("%Y-%m-%dT%H:%M:%S") + time.localtime().tm_zone + ".log"
        return os.path.join(logs_directory, log_name)

    @staticmethod
    def _createLogger(logger_name, eventFormat, dateFormat, level):
        """ Create a custom logger with the Logging library. """
        logger = logging.getLogger(logger_name)
        fileHandler = logging.FileHandler(logger_name)
        formatter = logging.Formatter(eventFormat, dateFormat)

        fileHandler.setFormatter(formatter)
        logger.setLevel(level)
        logger.addHandler(fileHandler)
    
        return logger

    def move_logs(self, dst): 
        new_log_path = os.path.join(dst, os.path.basename(self.log_file_path))
        move(self.log_file_path, new_log_path)
        self.log_file_path = new_log_path
        self.logging = self._createLogger(self.log_file_path, 
                "%(asctime)s [%(levelname)s] %(message)s", 
                "%m/%d/%Y %I:%M:%S %p",
                logging.DEBUG)

class LoggerWriter(object):

    """ Logger class destinated for stdout and stderr messages. """

    def __init__(self, logger) -> None:
        self._logger = logger
        self._buf = ""

    def write(self, buffer):
        """ Write each lines in the buffer with a logger. """
        self._buf += buffer
        while '\n' in self._buf:
            index = self._buf.find('\n')
            self._logger(self._buf[:index])
            print(self._buf[:index], file=sys.stdout)
            self._buf = self._buf[index + 1:]

    def flush(self):
        """ Flush / reset the buffer of stdout or stderr. """
        if self._buf != "":
            self._logger(self._buf)
            self._buf = ""