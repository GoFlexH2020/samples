#!/usr/bin/env python
#author mark_purcell@ie.ibm.com

import argparse
import json
import time
import sys

from goflexapi import GoFlexAPI

parser = argparse.ArgumentParser(description='Sample AMQP client')


def process_reply(msg):
    message = json.loads(msg)
    have_answer = None

    try:
        response = message['serviceResponse']
        service = response['service']
        code = service['status']

        if code is 202:
            print 'Request accepted.'
        elif code is 200:
            print 'Request complete.'
            print("Received %r" % message)
            have_answer = 1
        else:
            error = service['result']
            print 'Warning (%d): %s' % (code, error)
            have_answer = 1
    except Exception as e:
        print 'Error: %r ' % e

    #return None to continue waiting for messages indefinitely
    return have_answer


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
        #Read the template JSON for retrieving meter data
        with open('goflex-meterdata.json') as json_data:
            message = json.load(json_data)

        #Now connect to the messaging system
        api = GoFlexAPI(args.host, args.port, args.user, args.password, args.publish_topic, args.subscribe_topic)

        #Add the appropriate date range etc
        message['serviceRequest']['service']['args']['ts_id'] = 0
        message['serviceRequest']['service']['args']['from'] = "2009-07-13T00:00:00+0000"
        message['serviceRequest']['service']['args']['to'] = "2017-07-14T01:00:00+0000"

        #And send our message
        correlation_id = 1
        api.publish(message, correlation_id)
        print 'Sent: %r' % message

        #Now wait for the reply
        print('Waiting for messages. To exit press CTRL+C')
        api.receive(1, process_reply)
    except KeyboardInterrupt:
        print("Stopping")
    except Exception as e:
        print(e)

if __name__ == '__main__':
    main(sys.argv)


