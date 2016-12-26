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
    parser = ArgumentParser()
    parser.add_argument('--debug', action='store_true', default=False, dest='debug',
      help = 'Set DEBUG flag for extra output.')
    parser.add_argument('-f', '--file', action = 'store', dest = 'file', type = FileType('r'),
      help = 'Use file for extracting stats data.')
    parser.add_argument('--start', action = 'store', dest = 'start', default = datetime(1970,1,1,0,0,0),
      type = lambda d: datetime.strptime(d, '%Y-%m-%dT%H:%M:%S'),
      help = 'Extract rates after start time. (YYYY-MM-DDTHH:MM:SS)')
    parser.add_argument('--stop', action = 'store', dest = 'stop',
      type = lambda d: datetime.strptime(d + ':999999', '%Y-%m-%dT%H:%M:%S:%f'), default = None,
      help = 'Extract rates before stop time. (YYYY-MM-DDTHH:MM:SS)')
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
      AlcatrazStats\s+
      \[(?P<loglevel>.*)\]\s+
      %s
      """ % entry, re.VERBOSE)

    m = patt.search(line)
    if m :
      return m.groupdict()
    else :
      return None

  def process_file(self, spout) :
    with self.options.file as source :
      for stuff in self.group_by_date(source) :
        header = self.is_entry(spout, stuff[0])
        if header :
          date = datetime.strptime("%sT%s" % (header['date'], header['time']), '%Y-%m-%dT%H:%M:%S:%f')
          stats = self.get_rate(stuff[1:])
          if stats and date > self.options.start and ((self.options.stop == None) or (date < self.options.stop)) :
            print "%s: %d" % (date.strftime("%Y-%m-%d %H:%M:%S"), int(stats['total']))

  def main(self) :
    self.parse()
    # self.process_file('IngestionSpout')
    self.process_file('HoldSpout')

if __name__ == "__main__" :
  app = Statsparser()
  app.main()
