#!/usr/bin/env python

import sys
import optparse
import time
import datetime
from dateutil import parser as date_parser
from pymongo import MongoClient

class Throughput(object) :
  def __init__(self) :
    self.options = None
    self.mongoservers = ['fab-prev-mdb-h1:27017','fab-prev-mdb-h2:27017','fab-prev-mdb-h3:27017']
    self.client = None
    self.sleep = 10
    self.start = None
    self.stop = None

  def get_mongo(self) :
    if self.client == None :
      self.client = MongoClient(host = self.mongoservers)

    return self.client

  def parse(self) :
    parser = optparse.OptionParser()
    parser.add_option('--debug', help = 'Set DEBUG flag.', action = 'store_true', dest = 'debug', default = False)
    parser.add_option('-s', '--sleep', action = 'store', dest = 'duration', 
      type = 'int', help = 'Number of seconds to sleep between timings.', default = 10)
    parser.add_option('--start', help = 'Do timings starting from specific time (YYYY-MM-DDTHH:MM:SS).',
      type = 'string', dest = 'start', action = 'store', default = None)
    parser.add_option('--stop', help = 'Do timings ending at specific time (YYYY-MM-DDTHH:MM:SS).',
      type = 'string', dest = 'stop', action = 'store', default = None)
    (self.options, args) = parser.parse_args()
    self.sleep = self.options.duration
    if self.options.start != None :
      self.start = date_parser.parse(self.options.start)
    if self.options.stop != None :
      self.stop = date_parser.parse(self.options.stop)

  def dotiming(self, duration) :
    total1 = 0
    total2 = 0
    time1 = 0
    time2 = 0
    print "Throughput timing for %d seconds." % duration
    mongo = self.get_mongo()
    time1 = int(time.time())
    for result in list(mongo.FileGateway.audit_messages.aggregate([{'$group': {'_id': '$status', 'total': { '$sum': 1 }}}])) :
      if result['_id'] == u'SUCCESS' :
        total1 = result['total']
    time.sleep(duration)
    time2 = int(time.time())
    for result in list(mongo.FileGateway.audit_messages.aggregate([{'$group': {'_id': '$status', 'total': { '$sum': 1 }}}])) :
      if result['_id'] == u'SUCCESS' :
        total2 = result['total']

    print "Throughput: %.2f/sec" % ((total2 - total1) / (time2 - time1))

  def dorange(self, start, end) :
    total = 0
    print "Throughput timing from %s to %s." % (str(start.isoformat()), str(end.isoformat()))
    # print (end - start).total_seconds()
    mongo = self.get_mongo()
    for result in list(mongo.FileGateway.audit_messages.aggregate([{'$match' : {'audit_time' : {'$gte': start }}},
      {'$match' : {'audit_time' : {'$lte': end }}},{'$group':{'_id' : '$status', 'total': { '$sum': 1}} }])) :
      if result['_id'] == u'SUCCESS' :
        total = result['total']

    print "%d results over %d seconds." % (total, (end - start).total_seconds())
    print "Throughput: %.2f/sec" % (total / (end - start).total_seconds())
 

  def main(self) :
    self.parse()
    if self.start and self.stop :
      self.dorange(self.start, self.stop)
    else :
      self.dotiming(self.sleep)
    
    

app = Throughput()
app.main()
