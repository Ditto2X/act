#!/usr/bin/env python

import sys
from argparse import ArgumentParser, FileType
import re

class Stat(object) :
  def __init__(self) :
    self.options = None

  def parse(self) :
    """ Parses command line arguments. """
    parser = ArgumentParser()
    parser.add_argument('--debug', action='store_true', default=False, dest='debug',
      help = 'Set DEBUG flag for extra output.')
    parser.add_argument('-f', '--file', action='store', dest='file', type=FileType('r'),
      help = 'Use file for extracting stats data.')
    self.options = parser.parse_args()

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

  def get_rate(self, buffer) :
    """ Searches buffer for a line matching the rate statistics and returns rate and total. """
    patt = re.compile(r"""
      Ack\stotal=(?P<total>\d+)\s+
      rate=(?P<rate>\d+)
      """, re.VERBOSE)

    for line in buffer :
      m = patt.search(line)
      if m:
        return m.groupdict()

    return None

  def is_ingestion(self, line) :
    """ If line matches the pattern a hash is returned with the variables populated. """
    patt = re.compile(r"""
      (?P<date>\d{4}-\d{2}-\d{2})\s+
      (?P<time>\d{2}:\d{2}:\d{2}:\d{3})\s+
      (?P<host>\S+)\s+
      \[THREAD\sID=(?P<thread>.*)\]\s+
      AlcatrazStats\s+
      \[(?P<loglevel>.*)\]\s+
      IngestionSpout
      """, re.VERBOSE)

    m = patt.search(line)
    if m :
      return m.groupdict()
    else :
      return None

  def main(self) :
    self.parse()
    with self.options.file as source :
      for stuff in self.group_by_date(source) :
        header = self.is_ingestion(stuff[0])
        if header :
          stats = self.get_rate(stuff[1:])
          if stats :
            print "%s %s: %d" % (header['date'], header['time'], int(stats['total']))

if __name__ == "__main__" :
  app = Stat()
  app.main()
