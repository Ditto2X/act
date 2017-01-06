#!/usr/bin/env python

import sys
import argparse
from pymongo import MongoClient
from datetime import date, time, datetime, timedelta
import socket                          # for hostname
import getpass                         # for username
import smtplib
from email.mime.text import MIMEText

class IngestionCounts(object) :
  GB = 1024 * 1024 * 1024
  HOURS = 24

  def __init__(self) :
    self.options = None
    self.mongoservers = ['server1:27017', 'server2:27017', 'server3:27017']

  def parse(self) :
    """ Parse command line arguments. """
    parser = argparse.ArgumentParser()
    parser.add_argument('--debug', action= 'store_true', default=False, dest='debug',
      help = 'Set DEBUG flag for extra output.')
    parser.add_argument('--date', action = 'store', default = date.today() - timedelta(1),
      dest = 'date', type = lambda d: datetime.strptime(d, '%Y-%m-%d').date(), 
      help = 'Date to gather statistics for.  (YYYY-MM-DD)')
    parser.add_argument('--subject', default = 'Ingestion Report', type = str, dest = 'subject',
      help = 'The subject for the email to be sent.')
    parser.add_argument('--smtpserver', default = 'smtp.server.com', type = str, dest = 'smtpserver',
      help = 'SMTP server to use to deliver email.')
    parser.add_argument('--to', nargs = '+', default = ['user@server.com'], dest = 'emailto',
      help = 'To whom to send the email with the report.')
    self.options = parser.parse_args()

  def send_email(self, to, subject, text) :
    "Sends an email."
    msg = MIMEText(text, 'html')
    msg['Subject'] = subject
    msg['To'] = ', '.join(to)
    msg['From'] = str('@'.join([getpass.getuser(), socket.gethostname()]))
  
    s = smtplib.SMTP(self.options.smtpserver)
    s.sendmail(msg['From'], to, msg.as_string())
    s.quit()

  def main(self) :
    output = []
    self.parse()

    client = MongoClient(host = self.mongoservers)
    db = client['database']

    for t in range(self.HOURS) :
      begin = datetime.combine(self.options.date, time(t, 0))
      end = datetime.combine(self.options.date, time(t, 59, 59))
      output.append("%s,%s,%s,%s" % (begin.date().strftime('%m/%d/%Y'), begin.time().strftime('%H:%M'),
        end.time().strftime('%H:%M'), db.compliance_audit_received.find({"start_time": {"$gte": begin, "$lt": end}}).count()))

    ingested = sum(x['total'] for x in db.compliance_audit_received.aggregate(
      {'$group':{'_id' : "$channel", 'total': { '$sum': "$payload_size"}}})['result'])

    output.append(str(ingested / self.GB))

    docs = sum(x['total'] for x in db.compliance_audit_received.aggregate(
      [{'$match' : {"audit_time" : {'$gte': datetime(2013,1,23,0,0)}}},
      {'$group':{'_id' : "$cluster", 'total': { '$sum': 1}} }])['result'])

    output.append(str(docs))

    self.send_email(self.options.emailto, self.options.subject, "<BR>\n".join(output))

if __name__ == "__main__" :
  app = IngestionCounts()
  app.main()
