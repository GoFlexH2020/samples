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
                print("Received Rows: %r " % service['result']['count'])

                global devices
                devices = list(service['result']['ts_ids'])
                print("Devices: %r" % devices)
                print(devices[0])
            elif correlation >= 2:
                #{'status': 200, 'state': 'Finished', 'result': {'timeseriesMetadata': ['observedTimestampUTC', 'value'], 'timeseriesRows': xxx, 'timeseries': [[][]}, 'name': 'MeterDataRetrievalService'}

                rows = service['result']['count']
                data = service['result']['values']

                global ts_count
                ts_count += rows

                global ts
                ts.extend(data)
                print("Received Rows: %r " % service['result']['count'])
                print("Row 1: %r" % service['result']['values'][0])
                print("Row %d: %r" % (rows, service['result']['values'][rows-1]))

                #for x in range(0, rows):
                #    print(datetime.datetime.strptime(service['result']['timeseries'][x][0], '%Y-%m-%dT%H:%M:%S+00:00'))

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
                    "name": "TimeseriesService",
                    "args": {
                        "cmd": "ts/get_timeseries_values",
                        "device_id": meter,
                        "from": from_date,
                        "to": to_date
                    }
                }
            }
        }

        #And send our message
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


def request_meter_list(api, correlation):
    try:
        message = { 
            "serviceRequest": { 
                "service": { 
                    "name": "TimeseriesService",
                    "args": {
                        "cmd": "ts/get_time_series"
                    }
                }
            }
        }

        api.publish(message, correlation)
        print 'Sent: %r' % message

        #Now wait for the reply
        print('Waiting for meter listing. To exit press CTRL+C')
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
    parser.add_argument('--vhost', action='store', dest='vhost',
                 required=True, help='Virtual Host')
    parser.add_argument('--cert', action='store', dest='cert',
                 required=True, help='Certificate file')
    parser.add_argument('--publish', action='store', dest='publish_topic',
                 required=True, help='Publish topic')
    parser.add_argument('--subscribe', action='store', dest='subscribe_topic',
                 required=True, help='Subscribe topic')
    args = parser.parse_args()

    try:
        #Now connect to the messaging system
        api = GoFlexAPI(args.host, args.port, args.user, args.password, args.vhost, 
                          args.cert, args.publish_topic, args.subscribe_topic)

        request_meter_list(api, 1)

        global ts, ts_count
        request_meter_data(api, devices[0], "2001-07-13T00:00:00+00:00", "2020-08-13T01:00:00+00:00", 2)
    except KeyboardInterrupt:
        print("Stopping")
    except Exception as e:
        print(e)

if __name__ == '__main__':
    main(sys.argv)


