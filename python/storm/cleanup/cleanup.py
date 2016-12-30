#!/usr/bin/env python

import os
import sys
import re
import argparse
import shutil
import yaml
import fcntl
# from pymongo import MongoClient
from pymongo import Connection
from datetime import datetime

class Cleanup(object) :
  def __init__(self) :
    self.options = None
    self.client = None
    self.mongoservers = None
    self.databases = None
    self.directory = None
    self.config = None
    self.logfile = 'purge.' + datetime.now().strftime('%Y%m%d-%H%M')
    self.scriptdir = os.path.dirname(os.path.abspath(__file__))

    if os.path.exists(os.path.join(self.scriptdir, 'config.yaml')) :
      with open(os.path.join(self.scriptdir, 'config.yaml'), 'r') as yamlfile :
        self.config = yaml.load(yamlfile)

  def parse(self) :
    """ Parse command line arguments. """
    parser = argparse.ArgumentParser()
    parser.add_argument('--debug', action='store_true', default=False, dest='debug',
      help = 'Set DEBUG flag for extra output.')
    parser.add_argument('-d', '--dir', action = 'store', dest = 'dir',
      help = 'Directory to process.')
    parser.add_argument('--dbs', nargs = '+', type = str, dest = 'dbs', metavar = 'DB',
      help = 'Mongo Databases to query.')
    parser.add_argument('-m', '--mongo', nargs = '+', type = str, dest = 'mongo', metavar = 'SERVER:27017',
      help = 'MongoDB servers.')
    parser.add_argument('--purge', action = 'store_true', default = False, dest = 'purge',
      help = 'Purge valid entries from disk.')
    self.options = parser.parse_args()

    if self.mongoservers == None :
      if self.options.mongo :
        self.mongoservers = self.options.mongo
      elif self.config and ('mongoservers' in self.config.keys()) :
        self.mongoservers = self.config['mongoservers']
      else :
        print "Please specify mongo servers to connect to (--mongo)."
        sys.exit()

    if self.databases == None :
      if self.options.dbs :
        self.databases = self.options.dbs
      elif self.config and ('databases' in self.config.keys()) :
        self.databases = self.config['databases']
      else :
        print "Please specify mongo databases to check (--dbs)."
        sys.exit()

    if self.directory == None :
      if self.options.dir :
        self.directory = self.options.dir
      elif self.config and ('searchdir' in self.config.keys()) :
        self.directory = self.config['searchdir']
      else :
        print "Please specify a directory to search (-d)."
        sys.exit()

  def log(self, string) :
    """ Creates a directory "logs" where the executible lives and writes string to a file in that directory. """
    path = os.path.join(self.scriptdir, 'logs')

    if not os.path.isdir(path) :
      os.makedirs(path)

    with open(os.path.join(path, self.logfile), 'a') as logfile :
      logfile.write(string + "\n")

  def get_mongo(self) :
    """ Creates a connection to MongoDB and stores it for future use. """
    if self.client == None :
      # self.client = MongoClient(host = self.mongoservers)
      self.client = Connection(host = self.mongoservers)
    return self.client

  def check_mongo(self, id) :
    """ Check mongo if ID exists and the current status """
    states = [ 'CANCELLED', 'FAILED', 'COMPLETED', 'COMPLETEDWITHWARNING', 'MERGE_FAILED', 'UPLOAD_FAILED']
    mongo = self.get_mongo()

    for database in self.databases :
      db = mongo[database]
      result = db.job_request_config.find({'_id': id.split('_')[0]}) 
      if result.count() > 0 and (list(result)[0]['status'] in states) :
        return True
    return False

  def process_dir(self, location) :
    """ Check location (directory) for entries and verify that they're safe to delete. """
    patt = re.compile(r"""
      \w{8}-\w{4}-\w{4}-\w{4}-\w{12}
      """, re.VERBOSE)

    for directory in os.listdir(location) :
      if patt.match(directory) :
        if self.check_mongo(directory) :
          self.log("CANDIDATE: %s" %  directory)
          if self.options.purge :
            shutil.rmtree(os.path.join(location, directory))
            self.log("- DELETED: %s" % directory)
        else :
          self.log(" SKIPPING: %s" % directory)

  def lockdown(self) :
    lockfile = '/tmp/cleanup.lock'
    fp = open(lockfile, 'w')
    try :
      fcntl.lockf(fp, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except IOError :
      print "Process already running.  Exiting .."
      sys.exit(1)    
    return fp

  def main(self) :
    self.parse()
    fp = self.lockdown()
    self.process_dir(self.directory)
    fp.close()

if __name__ == "__main__" :
  app = Cleanup()
  app.main()
