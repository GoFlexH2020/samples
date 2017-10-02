#!/usr/bin/env python
#author mark_purcell@ie.ibm.com

import argparse
import json
import time
import sys
import datetime

from goflexapi import GoFlexAPI

parser = argparse.ArgumentParser(description='Sample AMQP client')

devices = []
ts = []
ts_count = 0


def process_result(message, service, code, correlation):
    if code == 200:
        print 'Request complete.'

        try:
            if correlation == 1:
                #{'status': 200, 'state': u'Finished', 'name': 'MeterListingService', 'result': {'deviceCount': xxx, 'deviceMetadata': ['Device Name'], 'devices': []}}
                print("Received Rows: %r " % service['result']['deviceCount'])

                global devices
                devices = list(service['result']['devices'])
                print("Devices: %r" % devices)
                print(devices[0])
            elif correlation >= 2:
                #{'status': 200, 'state': 'Finished', 'result': {'timeseriesMetadata': ['observedTimestampUTC', 'value'], 'timeseriesRows': xxx, 'timeseries': [[][]}, 'name': 'MeterDataRetrievalService'}

                rows = service['result']['timeseriesRows']
                if rows > 0:
                    data = service['result']['timeseries']

                    global ts, ts_count
                    ts_count += rows
                    ts.extend(data)
                    print("Received Rows: %r " % rows)
                    print("Row 1: %r" % service['result']['timeseries'][0][0])
                    print("Row %d: %r" % (rows, service['result']['timeseries'][rows-1]))
            else:
                raise Exception("Unknown correlation")
        except Exception as e:
            print 'Error: %r ' % e
    elif code > 400:
        error = service['result']
        print 'Error (%d): %s' % (code, error)
    else:
        error = service['result']
        print 'Warning (%d): %s' % (code, error)

    #return None to continue waiting for messages indefinitely
    return 1 



def request_meter_data(api, meter, from_date, to_date, correlation):
    try:
        message = { 
            "serviceRequest": { 
                "service": { 
                    "args": {
                        "device_id": meter,
                        "from": from_date,
                        "to": to_date
                    },
                    "name": "MeterDataRetrievalService"
                }
            }
        }

        #And send our message
        correlation += 1
        api.publish(message, correlation)
        print 'Sent: %r' % message

        #Now wait for the reply
        print('Waiting for meter data. To exit press CTRL+C')

        timeout_seconds = 30
        messages = api.receive(timeout_seconds, process_result)
        if messages is 0:
            print 'Timed out waiting for reply.'
            return
    except Exception as e:
        print('Error %r' % e) 


def main(argv=None):
    parser.add_argument('--host', action='store', dest='host',
                 required=True, help='Host name or ip')
    parser.add_argument('--port', action='store', dest='port',
                 required=True, help='Port number', type=int)
    parser.add_argument('--user', action='store', dest='user',
                 required=True, help='Username')
    parser.add_argument('--password', action='store', dest='password',
                 required=True, help='Password')
    parser.add_argument('--publish', action='store', dest='publish_topic',
                 required=True, help='Publish topic')
    parser.add_argument('--subscribe', action='store', dest='subscribe_topic',
                 required=True, help='Subscribe topic')
    args = parser.parse_args()

    try:
        #Now connect to the messaging system
        api = GoFlexAPI(args.host, args.port, args.user, args.password, args.publish_topic, args.subscribe_topic)

        message = { 
            "serviceRequest": { 
                "service": { 
                    "args": {},
                    "name": "MeterListingService"
                }
            }
        }

        correlation = 1
        api.publish(message, correlation)
        print 'Sent: %r' % message

        print('Waiting for meter listing. To exit press CTRL+C')
        timeout_seconds = 30
        messages = api.receive(timeout_seconds, process_result)
        if messages is 0:
            print 'Timed out waiting for reply.'
            return

        global ts, ts_count, devices
        request_meter_data(api, devices[0], "2001-07-13T00:00:00+00:00", "2020-08-13T01:00:00+00:00", correlation)
        request_meter_data(api, devices[0], ts[ts_count-1][0], "2020-08-13T01:00:00+00:00", correlation)
    except KeyboardInterrupt:
        print("Stopping")
    except Exception as e:
        print(e)

if __name__ == '__main__':
    main(sys.argv)


