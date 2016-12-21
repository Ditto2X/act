#!/usr/bin/env python

import sys
import optparse

class Generic(object) :
  def __init__(self) :
    self.options = None

  def parse(self) :
    parser = optparse.OptionParser()
    parser.set_defaults(debug = False)
    parser.add_option('--debug', help = 'Set DEBUG flag.', action = 'store_true', dest = 'debug')
    (self.options, args) = parser.parse_args()

  def main(self) :
    self.parse()

app = Generic()
app.main()
