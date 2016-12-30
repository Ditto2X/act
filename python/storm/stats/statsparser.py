#!/usr/bin/env python

import sys
from argparse import ArgumentParser, FileType
import re
from datetime import datetime

class Statsparser(object) :
  def __init__(self) :
    self.options = None

  def parse(self) :
    """ Parses command line arguments. """
    parser = ArgumentParser(usage='%(prog)s [options] (--ingestion|--hold)')
#     parser.add_argument('-f', '--file', action = 'store', dest = 'file', type = FileType('r'), required = True,
#       help = 'Use file for extracting stats data.')
    parser.add_argument('-f', '--file', nargs = '+', type = FileType('r'), required = True,
      help = 'Use file(s) for extracting stats data.')
    parser.add_argument('--debug', action='store_true', default=False, dest='debug',
      help = 'Set DEBUG flag for extra output.')
    parser.add_argument('--start', action = 'store', dest = 'start', default = datetime(1970,1,1,0,0,0),
      type = lambda d: datetime.strptime(d, '%Y-%m-%dT%H:%M:%S'),
      help = 'Extract rates after start time. (YYYY-MM-DDTHH:MM:SS)')
    parser.add_argument('--stop', action = 'store', dest = 'stop',
      type = lambda d: datetime.strptime(d + ':999999', '%Y-%m-%dT%H:%M:%S:%f'), default = None,
      help = 'Extract rates before stop time. (YYYY-MM-DDTHH:MM:SS)')
    group = parser.add_mutually_exclusive_group(required = True)
    group.add_argument('--ingestion', action = 'store_const', dest = 'action', const = 'IngestionSpout',
      help = 'Process IngestionSpout entries.')
    group.add_argument('--hold', action = 'store_const', dest = 'action', const = 'HoldSpout',
      help = 'Process HoldSpout entries.')
    self.options = parser.parse_args()

    if (self.options.stop != None) and (self.options.start > self.options.stop) :
      print "Start date must come before stop date."
      sys.exit()

  def group_by_date(self, source) :
    """ Groups a stream into buffers demarcated by lines starting with date and time. """
    patt = re.compile(r"""
      \d{4}-\d{2}-\d{2}\s+
      \d{2}:\d{2}:\d{2}:\d{3}
      """, re.VERBOSE)
    buffer = []

    for line in source :
      m = patt.search(line)
      if m :
        if buffer: yield buffer
        buffer = [ line.rstrip() ]
      else :
        buffer.append(line.rstrip())
    yield buffer

  # ComplianceSuccess total=122 rate=0/sec
  def get_rate(self, buffer) :
    """ Searches buffer for a line matching the rate statistics and returns rate and total. """
    patt = re.compile(r"""
      (Ack|ComplianceSuccess)\stotal=(?P<total>\d+)\s+
      rate=(?P<rate>\d+)
      """, re.VERBOSE)

    for line in buffer :
      m = patt.search(line)
      if m:
        return m.groupdict()

    return None

  def is_entry(self, entry, line) :
    """ If line matches the pattern a hash is returned with the variables populated. """
    patt = re.compile(r"""
      (?P<date>\d{4}-\d{2}-\d{2})\s+
      (?P<time>\d{2}:\d{2}:\d{2}:\d{3})\s+
      (?P<host>\S+)\s+
      \[THREAD\sID=(?P<thread>.*)\]\s+
      (?P<tag>\S+)\s+
      \[(?P<loglevel>.*)\]\s+
      %s
      """ % entry, re.VERBOSE)

    m = patt.search(line)
    if m :
      return m.groupdict()
    else :
      return None

  def process_file(self, spout) :
    for source in self.options.file :
      total = 0
      begin,end = None, None
      for stuff in self.group_by_date(source) :
        header = self.is_entry(spout, stuff[0])
        if header :
          date = datetime.strptime("%sT%s" % (header['date'], header['time']), '%Y-%m-%dT%H:%M:%S:%f')
          stats = self.get_rate(stuff[1:])
          if stats and date > self.options.start and ((self.options.stop == None) or (date < self.options.stop)) :
            if begin == None or date < begin :
              begin = date
            if end == None or date > end :
              end = date
            total += int(stats['total'])
            # print "%s: %d" % (date.strftime("%Y-%m-%d %H:%M:%S"), int(stats['total']))
      print "Total: %d, Diff = %s" % (total, end - begin)

  def main(self) :
    self.parse()
    self.process_file(self.options.action)

if __name__ == "__main__" :
  app = Statsparser()
  app.main()
