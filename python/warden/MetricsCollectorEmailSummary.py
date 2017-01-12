import MySQLdb
import simplejson
import time
from datetime import datetime, timedelta

if not __name__ == "__main__" :
    from app.Cache2 import Cache2
    from app.Logger import Logger
    from app import app

class MetricsCollectorEmailSummary(object) :

    def __init__(self, dodebug = False) :
        """
        """
        self.BYDATE     = 0

        self.TODAY      = 0
        self.DAILY      = 1
        self.ALL        = 2

        self.DAYSBACK   = 7

        self.DEBUG      = dodebug
        self.codes      = {}
        self.mysqlhost  = None

        if self.DEBUG :
            self.mysqlhost  = "fab-jpmc-warden-h5"
        else:
            if not app.config['NODES'] or not app.config['NODES']['ALCATRAZ_METRICS']['nodes'][0] :
                return
            else:
                self.mysqlhost = app.config['NODES']['ALCATRAZ_METRICS']['nodes'][0]
                self.DEBUG_PRINT("Trying MySQL Node: " + self.mysqlhost)

        self.loadErrorCodes()

    def DEBUG_PRINT(self, string) :
        """
        """
        if self.DEBUG :
            print string
        else:
            Logger.log.debug(string)

    def commify(self, value) :
        """
        """
        return "{:,}".format(value)

    def doQuery(self, query) :
        """
        """
        try:
            connection = MySQLdb.connect(self.mysqlhost, user = "opsuser", passwd = "alcatraz1400",
                db = "ALCATRAZ_METRICS")
            with connection as cursor :
                cursor.execute(query)
                data = cursor.fetchall()
                return data
        except Exception as e:
            print "Uh Oh: ", e.message

        return []

    def loadByDate(self) :
        """
        """
        results = {}
        fromdate = (datetime.now() + timedelta(-self.DAYSBACK)).date()
        query = ("select DATE_ID, ERROR_CODE, sum(ERROR_COUNT)"
                 " from EGW_MONITORING"
                 " WHERE DATE_ID >= '" + str(fromdate) + "'"
                 " group by DATE_ID, ERROR_CODE"
                 " order by DATE_ID")

        data = self.doQuery(query)

        for row in data :
            (date, code, count) = row

            if not results.get(date, None) :
                results[date] = []

            results[date].append([int(code), self.codes[code], int(count)])

        return results

    def loadErrorCodes(self) :
        """
        """
        query = ("SELECT ERROR_CODE, ERROR_MESSAGE FROM EGW_ERROR_MESSAGES")

        data = self.doQuery(query)

        for row in data :
            (code, message) = row
            self.codes[code] = str(message)


    def loadDailyEmail(self) :
        """
        """
        results = []
        query = ("SELECT DATE_ID, ERROR_CODE, sum(ERROR_COUNT)"
                 " FROM EGW_MONITORING"
                 " WHERE ERROR_CODE=200"
                 " GROUP BY DATE_ID, ERROR_CODE"
                 " ORDER BY DATE_ID DESC LIMIT 0,14")

        data = self.doQuery(query)

        for row in data :
            (date, error, count) = row
            results.append([date.strftime('%Y-%m-%d'), int(error), int(count)])

        return results

    def loadAllRecords(self) :
        """
        """
        results = []
        query = ("SELECT ERROR_CODE, SUM(ERROR_COUNT) FROM EGW_MONITORING GROUP BY ERROR_CODE")

        data = self.doQuery(query)

        for row in data :
            (code, total) = row
            results.append([int(code), self.codes[code], int(total)])

        return results

    def loadDailySummary(self) :
        """
        """
        response = []
        query = ("select * from EGW_SUMMARY_METRICS")

        for row in self.doQuery(query) :
            (period, count, change, date) = row
            response.append([period, count, change + '%'])

        return response

    def GetEmailSummary(self) :
        """
        """
        response = {
            "dataSets": [
                {
                    "label": "Today's Summary",
                    "data": {
                        "columns": [
                            ["string", "Period"],
                            ["string", "Count"],
                            ["string", "% Change"]
                        ],
                        "rows": []
                    }
                },
                {
                    "label": "Daily Email Summary",
                    "data": {
                        "columns": [
                            ["string", "Date"],
                            ["number", "Error Code"],
                            ["number", "Total"]
                        ],
                        "rows": None
                    }
                },
                {
                    "label": "All Records",
                    "data": {
                        "columns": [
                            ["number", "Error Code"],
                            ["string", "Message"],
                            ["number", "Total"]
                        ],
                        "rows": None
                    }
                }
            ]
        }

        self.DEBUG_PRINT("SUMMARY: Loading data ..")

        self.loadDailySummary()
        response['dataSets'][self.TODAY]['data']['rows'] = self.loadDailySummary()
        response['dataSets'][self.DAILY]['data']['rows'] = self.loadDailyEmail()
        response['dataSets'][self.ALL]['data']['rows'] = self.loadAllRecords()

        if self.DEBUG :
            print simplejson.dumps(response)
        else:
            self.DEBUG_PRINT("SUMMARY: Updating Cache.")
            Cache2._EMAILSummary = response
  
    def GetEmailHourly(self) :
        """
        """
        response = {
            "dataSets": [
            ]
        }

        self.DEBUG_PRINT("DAILY: Loading data ..")

        results = self.loadByDate()

        keys = results.keys()
        keys.sort(reverse = True)

        for date in keys :
            datum = {
                "label": date.strftime('%Y-%m-%d'),
                "data": {
                    "columns": [
                        ["number", "Error Code"],
                        ["string", "Error Text"],
                        ["number", "Count"]
                    ],
                    "rows": []
                }
            }

            for item in results[date] :
                (code, message, count) = item
                datum['data']['rows'].append([int(code), str(message), int(count)])

            response['dataSets'].append(datum)

        if self.DEBUG :
            print simplejson.dumps(response)
        else:
            self.DEBUG_PRINT("DAILY: Updating Cache.")
            Cache2._EMAILDaily = response


            

if __name__ == "__main__" :
    app = MetricsCollectorEmailSummary(True)
    app.GetEmailSummary()
#    app.GetEmailHourly()
