#!/usr/bin/env python

#  Author:
#    Date:
# Purpose:

import sys
import argparse

class Generic(object) :
  def __init__(self) :
    self.options = None

  def parse(self) :
    """ Parse command line arguments. """
    parser = argparse.ArgumentParser()
    parser.add_argument('--debug', action='store_true', default=False, dest='debug',
      help = 'Set DEBUG flag for extra output.')
    self.options = parser.parse_args()

  def main(self) :
    self.parse()

if __name__ == "__main__" :
  app = Generic()
  app.main()
