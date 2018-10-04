#!/usr/bin/env python
#author robertgo@ie.ibm.com

import json
import time
import sys

from messages import GoFlexMessageFormatter
from goflexapi import GoFlexAPI


def process_result(message, service, code, correlation):
    if code == 200:
        print 'Request complete:'

        try:
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


def main(argv=None):
    try:
        #Now connect to the messaging system
        api = GoFlexAPI(argv) 
        formatter = GoFlexMessageFormatter()

        msg = formatter.keyValueService('put', [
                                                    ["simpleKeyValueInc","0"],
                                                    ["simpleKeyValueAvg","0"],
                                                    ["jsonKeyValue","{\"json\":\"value\"}"]
                                                ]
        )
        api.invoke_service(msg, 1, process_result, timeout=30)
		
        msg = formatter.keyValueService('get', [
                                                    ["simpleKeyValueInc"],
                                                    ["simpleKeyValueAvg"],
                                                    ["jsonKeyValue"]
                                                ]
        )
        api.invoke_service(msg, 2, process_result, timeout=30)

        msg = formatter.keyValueService('del', [
                                                    ["simpleKeyValueInc"],
                                                    ["simpleKeyValueAvg"],
                                                    ["jsonKeyValue"]
                                                ]
        )
        api.invoke_service(msg, 3, process_result, timeout=30)
    except KeyboardInterrupt:
        print("Stopping")
    except Exception as e:
        print(e)

if __name__ == '__main__':
    main(sys.argv)



