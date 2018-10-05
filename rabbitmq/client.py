#!/usr/bin/env python
#author mark_purcell@ie.ibm.com

from __future__ import print_function
import json
import time
import sys
import timeit
import datetime

from messages import GoFlexMessageFormatter
from goflexapi import GoFlexAPI

devices = []
ts = []
ts_count = 0


def process_result(message, service, code, correlation):
    if code == 200:
        print('Request complete:', correlation)

        try:
            if correlation == 1:
                print("Received Rows: %r " % service['result']['count'])

                global devices
                devices = list(service['result']['ts_ids'])
                print("Devices: %r" % devices)
                print(devices[0])
            elif correlation == 2:
                rows = service['result']['count']
                data = service['result']['values']

                global ts_count
                ts_count += rows

                global ts
                ts.extend(data)
                print("Received Rows: %r " % service['result']['count'])
                print("Row 1: %r" % service['result']['values'][0])
                print("Row %d: %r" % (rows, service['result']['values'][rows-1]))
            elif correlation == 3:
                print(message)
                pass
            else:
                raise Exception("Unknown correlation")
        except Exception as e:
            print('Error: %r ' % e)
    elif code > 400:
        error = service['result']
        print('Error (%d): %s' % (code, error))
    else:
        error = service['result']
        print('Warning (%d): %s' % (code, error))

    #return None to continue waiting for messages indefinitely
    return 1 



def main(argv=None):
    try:
        #Now connect to the messaging system
        api = GoFlexAPI(argv)
        formatter = GoFlexMessageFormatter()
        msg = formatter.request_meter_list()
        duration = api.invoke_service(msg, 1, process_result, timeout=30)
        print('Duration: ', duration)

        global ts, ts_count
        msg = formatter.request_meter_data(devices[0], "2001-07-13T00:00:00+00:00", "2020-08-13T01:00:00+00:00")
        duration = api.invoke_service(msg, 2, process_result, timeout=30)
        print('Duration: ', duration)

        values = [ {'ts_id':'my-time-series-id','observed_timestamp':'2018-10-05T11:07:00+00:00', 'value': 21.1 } ]
        msg = formatter.store_time_series(values)
        duration = api.invoke_service(msg, 3, process_result, timeout=30)
        print('Duration: ', duration)

        msg = formatter.request_meter_data('my-time-series-id', "2018-10-05T11:00:00+00:00", "2020-08-13T01:00:00+00:00")
        duration = api.invoke_service(msg, 2, process_result, timeout=30)
        print('Duration: ', duration)
    except KeyboardInterrupt:
        print("Stopping")
    except Exception as e:
        print(e)

if __name__ == '__main__':
    main(sys.argv)


