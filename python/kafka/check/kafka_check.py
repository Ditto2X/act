#!/usr/bin/env python

import os
import MySQLdb
import sys
import time
import argparse
import socket                          # for hostname
import getpass                         # for username
import smtplib
from email.mime.text import MIMEText
import yaml

config = {}
options = {}

# Number of seconds in each hour
class HOURS :
  ONE   = 60 * 60
  TWO   = ONE * 2
  THREE = ONE * 3
  FOUR  = ONE * 4
  FIVE  = ONE * 5
  SIX   = ONE * 6

class ALERT :
  OKAY  = 0
  WARN  = 1
  CRIT  = 2

def elapsed(seconds) :
  "Return elapsed time until now."
  return int(time.time()) - seconds

def epochtostring(seconds) :
  "Convert epoch time to string."
  return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(seconds))

def makedifftime(seconds) :
  "Returns a string showing days-hours:minutes:seconds given total seconds from now."
  r, s = divmod(elapsed(seconds), 60)
  r, m = divmod(r, 60)
  d, h = divmod(r, 24)
  return "%02d-%02d:%02d:%02d" % (d, h, m, s)

def DEBUG_PRINT(string) :
  "Print string only if options.DEBUG == True"
  if options.DEBUG :
    print string

def loaddata(timerange) :
  "Load data from MySQL and return a hash with results."
  results = {}
  query = ("SELECT concat(CONSUMER, '-', TOPIC) as SOURCE, PARTITION,"
           "  OFFSET, LOG_SIZE, LAG, CHECK_TIME"
           " FROM KAFKA_MONITORING"
           " WHERE CHECK_TIME > " + str(timerange) +
           " ORDER BY CHECK_TIME ASC")
  connection = MySQLdb.connect (host = "localhost", user = "opsuser", passwd = "alcatraz1400", 
    db = "ALCATRAZ_METRICS")
  cursor = connection.cursor()
  cursor.execute(query)
  data = cursor.fetchall()
  for row in data :
    (source, partition, offset, log_size, lag, check_time) = row
    DEBUG_PRINT("(%s, %d, %d, %d, %d, %s)" % (source, partition, offset, log_size, lag, epochtostring(check_time)))

    # Create the subhash (source) if it doesn't exist.
    if not source in results :
      results[source] = {}

    # Create the subhash (partition) if it doesn't exist.
    if not partition in results[source] :
      results[source][partition] = {
        'OFFSET' : offset,   'OFFSET_LAST' : -1, 'OFFSET_CHANGED' : check_time,
        'LOG'    : log_size, 'LOG_LAST'    : -1, 'LOG_CHANGED'    : check_time,
        'LAG'    : lag,      'LAG_LAST'    : -1, 'LAG_CHANGED'    : check_time }

    # If OFFSET is different from last, update it.
    if results[source][partition]['OFFSET'] != offset :
      results[source][partition]['OFFSET_LAST'] = results[source][partition]['OFFSET']
      results[source][partition]['OFFSET'] = offset
      results[source][partition]['OFFSET_CHANGED'] = check_time

    # If LOG is different from last, update it.
    if results[source][partition]['LOG'] != log_size :
      results[source][partition]['LOG_LAST'] = results[source][partition]['LOG']
      results[source][partition]['LOG'] = log_size
      results[source][partition]['LOG_CHANGED'] = check_time

    # If LAG is different from last, update it.
    if results[source][partition]['LAG'] != lag :
      results[source][partition]['LAG_LAST'] = results[source][partition]['LAG']
      results[source][partition]['LAG'] = lag
      results[source][partition]['LAG_CHANGED'] = check_time

  cursor.close()
  connection.close()
  return results

def check(source, thresholds) :
  "Implementing Alan's logic."
  size = len(source.keys())
  half = int(size / 2)
  results = [{'LOG' : ALERT.OKAY, 'OFFSET' : ALERT.OKAY, 'LAG' : ALERT.OKAY} for x in range(size)]
  errortext = []

  DEBUG_PRINT("Size = %d, half = %d" % (size, half))
  DEBUG_PRINT("Thresholds = %s" % str(thresholds))

  for partition in source :
    # No Data coming in.  Data Flow Issue.
    if (source[partition]['LOG'] == source[partition]['LOG_LAST']) and (source[partition]['LAG'] == 0) :
      if elapsed(source[partition]['LOG_CHANGED']) > HOURS.THREE :
        DEBUG_PRINT("- %02d: ERROR: No data coming in." % partition)
        errortext.append("- Partition %02d: No data coming in." % partition)
        results[partition]['LOG'] = ALERT.WARN

    # Queue Building Up:
    if source[partition]['OFFSET'] == source[partition]['OFFSET_LAST'] :
      if source[partition]['LAG'] > source[partition]['LAG_LAST'] :
        DEBUG_PRINT("- %02d: WARNING: Offset has not moved, but lag is moving." % partition)
        errortext.append("- Partition %02d: Offset has not moved, but lag is moving." % partition )
        results[partition]['OFFSET'] = ALERT.WARN
      else :
        DEBUG_PRINT("- %02d: ERROR: Offset and Lag are not moving since last checked." % partition)
        errortext.append("- Partition %02d: Offset and Lag are not moving." % partition )
        results[partition]['OFFSET'] = ALERT.CRIT
    elif source[partition]['OFFSET'] > source[partition]['OFFSET_LAST'] :
      DEBUG_PRINT("- %02d: SUCCESS: Offset is moving - All things are good." % partition)

    # Need Big Lag Condtiion
    if source[partition]['LAG'] > thresholds['LAG'] :
      DEBUG_PRINT("- %02d: ERROR: Lag above %d: %d" % (partition, thresholds['LAG'], source[partition]['LAG']))
      errortext.append("- Partition %02d: Lag = %d." % (partition, source[partition]['LAG']))
      results[partition]['LAG'] = ALERT.WARN

  DEBUG_PRINT("Alert results: LOG = %d, OFFSET = %d, LAG = %d" % (
    sum(1 for partition in results if partition['LOG'] > ALERT.OKAY),
    sum(1 for partition in results if partition['OFFSET'] > ALERT.OKAY), 
    sum(1 for partition in results if partition['LAG'] > ALERT.OKAY)))
    

  # Return an array of results.
  return [sum(1 for partition in results if partition['LOG']    > ALERT.OKAY) > half, # LOG ISSUE    :(more than 50%)
          sum(1 for partition in results if partition['OFFSET'] > ALERT.OKAY) > 0,    # OFFSET ISSUE :(any)
          sum(1 for partition in results if partition['LAG']    > ALERT.OKAY) > half, # LAG ISSUE    :(more than 50%)
          results, errortext]                                                         # RAW DATA

def send_email(to, subject, text) :
  "Sends an email."
  DEBUG_PRINT("SEND_EMAIL")
  msg = MIMEText(text, 'html')
  msg['Subject'] = subject
  msg['To'] = ','.join(to)
  msg['From'] = str('@'.join([getpass.getuser(), socket.gethostname()]))

  s = smtplib.SMTP('smtp.actiance.local')
  s.sendmail(msg['From'], msg['To'], msg.as_string())
  s.quit()

def parse() :
  "Parse command line arguments."
  global options
  parser = argparse.ArgumentParser()
  parser.add_argument('--debug', action='store_true', default=False, dest='DEBUG',
    help = 'Set DEBUG flag for extra output.')
  parser.add_argument('--dump', action = 'store_true', default=False, dest = 'DUMP')
  parser.add_argument('-e', '--emailto', nargs = '+', default = config.get('emailto', 
    ['dwarness@actiance.com']), dest = 'emailto', help = 'To whom to send alerts to.')
  parser.add_argument('-s', '--smtp', action = 'store', default = config.get('smtpserver', 
    'smtp.actiance.local'), dest = 'smtp', help = 'SMTP server to use.')
  options = parser.parse_args()

def build_threshold(source) :
  defaults = {'LAG': 50, 'OFFSET': 0, 'LOG': 0 }
  threshold = {}

  if source in config['threshold'].keys() :
    for key in defaults :
      threshold[key] = config['threshold'][source].get(key, defaults[key])
    return threshold

  return defaults

def main() :
  global config
  # Load external configuration file if one exists.
  configyaml = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'kafka_check.yaml')
  if os.path.exists(configyaml) :
    with open(configyaml, 'r') as yamlfile :
      config = yaml.load(yamlfile)

  # Parse command line arguments.
  parse()

  # Load data from the past four hours from the database.
  results = loaddata(elapsed(HOURS.FOUR))
  problems = []

  for source in results :
    DEBUG_PRINT("%-45s:" % source)
    log_issue, offset_issue, lag_issue, values, errors = check(results[source], build_threshold(source))
    if log_issue or offset_issue or lag_issue :
      problems.append("%s has the following issues:" % source)
      problems.extend(errors)
    DEBUG_PRINT(str(values))

  DEBUG_PRINT("Email recepients: %s" % ','.join(options.emailto))
  DEBUG_PRINT("Email Server: %s" % options.smtp)
  if len(problems) > 0 and not options.DEBUG :
    sources = "<BR>\n".join(problems)
    mail_text = ("There were issues with the following Kafka queues:<BR><BR>" + sources)
    send_email(options.emailto, 'Kafka issues ..', mail_text)

  sys.exit()

if __name__ == "__main__" :
  main()
