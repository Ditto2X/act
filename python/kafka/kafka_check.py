#!/usr/bin/env python

import MySQLdb
import sys
import time
import optparse
import socket                          # for hostname
import getpass                         # for username
import smtplib
from email.mime.text import MIMEText

DEBUG = False
DUMP = False

# Who to send emails to
emailto = ['dwarness@actiance.com','afiaccone@actiance.com']

# Threshold that lag becomes an issue
class THRESHOLD :
  LAG   = 10

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
  "Print string only if DEBUG == True"
  if DEBUG :
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

def check(source) :
  "Implementing Alan's logic."
  size = len(source.keys())
  half = int(size / 2)
  results = [{'LOG' : ALERT.OKAY, 'OFFSET' : ALERT.OKAY, 'LAG' : ALERT.OKAY}] * size
  errortext = []

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
    if source[partition]['LAG'] > THRESHOLD.LAG :
      DEBUG_PRINT("- %02d: ERROR: Lag above %d: %d" % (partition, THRESHOLD.LAG, source[partition]['LAG']))
      errortext.append("- Partition %02d: Lag = %d." % (partition, source[partition]['LAG']))
      results[partition]['LAG'] = ALERT.WARN

  # Return an array of results.
  return [sum(1 if partition['LOG']    > ALERT.OKAY else 0 for partition in results) > half, # LOG ISSUE    :(more than 50%)
          sum(1 if partition['OFFSET'] > ALERT.OKAY else 0 for partition in results) > 0,    # OFFSET ISSUE :(any)
          sum(1 if partition['LAG']    > ALERT.OKAY else 0 for partition in results) > half, # LAG ISSUE    :(more than 50%)
          results, errortext]                                                                # RAW DATA

def send_email(to, subject, text) :
  "Sends an email."
  DEBUG_PRINT("SEND_EMAIL")
  msg = MIMEText(text, 'html')
  msg['Subject'] = subject
  msg['To'] = ','.join(to)
  msg['From'] = str('@'.join([getpass.getuser(), socket.gethostname()]))

  # msg.add_header('Content-Type','text/plain')
  s = smtplib.SMTP('smtp.actiance.local')
  s.sendmail(msg['From'], msg['To'], msg.as_string())
  s.quit()

def parse() :
  "Parse command line arguments."
  global DEBUG
  global DUMP
  parser = optparse.OptionParser()
  parser.set_defaults(debug = False, dump = False)
  parser.add_option('--debug', action = 'store_true', dest = 'debug')
  parser.add_option('--dump', action = 'store_true', dest = 'dump')
  (options, args) = parser.parse_args()
  DEBUG = options.debug
  DUMP = options.dump

parse()
results = loaddata(elapsed(HOURS.FOUR))
problems = []

for source in results :
  DEBUG_PRINT("%-45s:" % source)
  log_issue, offset_issue, lag_issue, values, errors = check(results[source])
  if log_issue or offset_issue or lag_issue :
    problems.append("%s has the following issues:" % source)
    problems.extend(errors)
  DEBUG_PRINT(str(values))

if len(problems) > 0 :
  sources = "<BR>\n".join(problems)
  mail_text = ("There were issues with the following Kafka queues:<BR><BR>" + sources)
  send_email(emailto, 'Kafka issues ..', mail_text)

if DUMP :
  for source in results :
    for partition in results[source] :
      log_changed = results[source][partition]['LOG_CHANGED']
      print "%-45s[%02d]: %d (%s)" % (source, partition, log_changed, makedifftime(log_changed))

sys.exit()
