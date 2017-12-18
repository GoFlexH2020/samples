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


def keyValueServicePut(api, correlation):
    try:
        message = { 
            "serviceRequest": { 
                "service": { 
				    "name": "KeyValueService",
                    "args": {
						"cmd":"put",
						"keys":[
							["simpleKeyValueInc","0"],
							["simpleKeyValueAvg","0"],
							["jsonKeyValue","{\"json\":\"value\"}"]
						]
					}
                }
            }
        }

        api.publish(message, correlation)
        print 'Sent: %r' % message

        #Now wait for the reply
        print('Waiting for reply. To exit press CTRL+C')
        timeout_seconds = 10
        messages = api.receive(timeout_seconds, process_result)
        if messages is 0:
            print 'Timed out waiting for reply.'
            return
    except Exception as e:
        print('Error %r' % e) 
		
		
def keyValueServiceDel(api, correlation):
    try:
        message = { 
            "serviceRequest": { 
                "service": { 
				    "name": "KeyValueService",
                    "args": {
						"cmd":"del",
						"keys":[
							["simpleKeyValueInc"],
							["simpleKeyValueAvg"],
							["jsonKeyValue"]
						]
					}
                }
            }
        }

        api.publish(message, correlation)
        print 'Sent: %r' % message

        #Now wait for the reply
        print('Waiting for reply. To exit press CTRL+C')
        timeout_seconds = 10
        messages = api.receive(timeout_seconds, process_result)
        if messages is 0:
            print 'Timed out waiting for reply.'
            return
    except Exception as e:
        print('Error %r' % e) 
		
def keyValueServiceGet(api, correlation):
    try:
        message = { 
            "serviceRequest": { 
                "service": { 
				    "name": "KeyValueService",
                    "args": {
						"cmd":"get",
						"keys":[
							["simpleKeyValueInc"],
							["simpleKeyValueAvg"],
							["jsonKeyValue"]
						]
					}
                }
            }
        }

        api.publish(message, correlation)
        print 'Sent: %r' % message

        #Now wait for the reply
        print('Waiting for reply. To exit press CTRL+C')
        timeout_seconds = 10
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

        keyValueServicePut(api, 1)
		# simpleKeyValueInc = 0
		# simpleKeyValueAvg = 0
		# jsonKeyValue   = "{"json":"value"}"
		
        keyValueServiceGet(api, 2)
		# simpleKeyValueInc = 1
		# simpleKeyValueAvg = 10
		# jsonKeyValue   = "{"json":"value"}"

        keyValueServiceDel(api, 3)
		# simpleKeyValueInc = undefined
		# simpleKeyValueAvg = undefined
		# jsonKeyValue   = undefined
		
    except KeyboardInterrupt:
        print("Stopping")
    except Exception as e:
        print(e)

if __name__ == '__main__':
    main(sys.argv)



