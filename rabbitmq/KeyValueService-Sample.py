#!/usr/bin/env python
#author robertgo@ie.ibm.com

import argparse
import json
import time
import sys

from goflexapi import GoFlexAPI

parser = argparse.ArgumentParser(description='Sample AMQP client')


def process_result(message, service, code, correlation):
    if code == 200:
        print 'Request complete:'

        try:
            global result
            result = list(service['result'])
            print("Result: %r" % result)
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



def keyValueService(api, cmd, keys, correlation):
    try:
        message = {
            "serviceRequest": {
                "service": {
                    "name": "KeyValueService",
                    "args": {
                        "cmd": cmd,
                        "keys": keys
                    }
                }
            }
        }

        api.publish(message, correlation)
        messages = api.receive(10, process_result)
        if messages is 0:
            print 'Timed out waiting for reply.'
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

        keyValueService(api, 'put', [
                                        ["simpleKeyValueInc","0"],
                                        ["simpleKeyValueAvg","0"],
                                        ["jsonKeyValue","{\"json\":\"value\"}"]
                                    ], 1)
		
        keyValueService(api, 'get', [
                                        ["simpleKeyValueInc"],
                                        ["simpleKeyValueAvg"],
                                        ["jsonKeyValue"]
                                    ], 2)

        keyValueService(api, 'del', [
                                        ["simpleKeyValueInc"],
                                        ["simpleKeyValueAvg"],
                                        ["jsonKeyValue"]
                                    ], 3)
	
    except KeyboardInterrupt:
        print("Stopping")
    except Exception as e:
        print(e)

if __name__ == '__main__':
    main(sys.argv)



