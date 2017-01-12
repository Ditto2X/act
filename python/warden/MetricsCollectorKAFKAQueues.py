import MySQLdb
import simplejson
import time
from datetime import datetime

from app.Cache2 import Cache2
from app.Logger import Logger
from app import app

class MetricsCollectorKAFKAQueues(object) :
  def __init__(self) :
    """
    """

  def epochtostring(self, seconds) :
    "Convert epoch time to string."
    return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(seconds))

  def loaddata(self, timerange) :
    "Load data from MySQL and return a hash with results."
    results = {}
    mysqlhost = None
    query = ("SELECT concat(CONSUMER, '-', TOPIC) as SOURCE, PARTITION,"
             "  OFFSET, LOG_SIZE, LAG, CHECK_TIME"
             " FROM KAFKA_MONITORING"
             " WHERE CHECK_TIME > " + str(timerange) +
             " ORDER BY CHECK_TIME ASC")

    if not app.config['NODES'] or not app.config['NODES']['ALCATRAZ_METRICS']['nodes'][0] :
      return
    else:
      mysqlhost = app.config['NODES']['ALCATRAZ_METRICS']['nodes'][0]
      Logger.log.debug("Trying MySQL Node:" + mysqlhost)

#     mysqlhost = "fab-jpmc-warden-h5"

    connection = MySQLdb.connect (host = mysqlhost, user = "opsuser", passwd = "alcatraz1400",
      db = "ALCATRAZ_METRICS")
    cursor = connection.cursor()
    cursor.execute(query)
    data = cursor.fetchall()
    for row in data :
      (source, partition, offset, log_size, lag, check_time) = row
  
      # Create the subhash (source) if it doesn't exist.
      if not source in results :
        results[source] = {}
  
      # Create the subhash (partition) if it doesn't exist.
      if not partition in results[source] :
        results[source][partition] = {
          'OFFSET' : offset,   'OFFSET_LAST' : 0, 'OFFSET_CHANGED' : check_time,
          'LOG'    : log_size, 'LOG_LAST'    : 0, 'LOG_CHANGED'    : check_time,
          'LAG'    : lag,      'LAG_LAST'    : 0, 'LAG_CHANGED'    : check_time }
  
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

  def GetKafkaQueues(self) :
    """
    """
    response = {
      "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
      "data": {
        "columns": [
          ["string", "Queue"],
          ["number", "Partition"],
          ["number", "Lag"],
          ["string", "Lag Changed"],
          ["number", "Offset"],
          ["string", "Offset Changed"],
          ["number", "Log"],
          ["string", "Log Changed"]],
        "rows": []}}

    Logger.log.debug("In GetKafkaQueues")

    results = self.loaddata(int(time.time()) - (60 * 60 * 4))

    Logger.log.debug("Data received, formatting ..")
    for source in results :
      for partition in results[source] :
        temp = results[source][partition]
        data = [source, partition, temp['LAG'], self.epochtostring(temp['LAG_CHANGED']), temp['OFFSET'],
          self.epochtostring(temp['OFFSET_CHANGED']), temp['LOG'], self.epochtostring(temp['LOG_CHANGED'])]
        response['data']['rows'].append(data)

    Logger.log.debug("Updating Cache.")

    Cache2._KAFKAQueues = response

if __name__ == "__main__" :
  app = MetricsCollectorKAFKAQueues()
  app.GetKafkaQueues()
