#!/usr/bin/env python
#author robertgo@ie.ibm.com

from __future__ import print_function
import argparse
import json
import sys
import timeit

from messages import GoFlexMessageFormatter
from goflexapi import GoFlexAPI

parser = argparse.ArgumentParser(description='Sample AMQP client')

devices = []
ts = []
ts_count = 0


def process_result(message, service, code, correlation):
    print(message)
    if code == 200:
        print('Request complete:')

        try:
            global result
            #print("Result: %r" % json.dumps(service['result']))
        except Exception as e:
            print('Error: %r ' % e)
    elif code > 400:
        error = service['result']
        print('Error (%d): %s' % (code, error))
    else:
        error = service['result']
        print('Warning (%d): %s' % (code, error))

    # return None to continue waiting for messages indefinitely
    return 1 



def main(argv=None):
    parser.add_argument('--twodayhourlyforecast_api_key', action='store', dest='twodayhourlyforecast_api_key',
                        required=True, help='Two-day Hourly Weather Forecast API Key')
    parser.add_argument('--solar15dayforecast_api_key', action='store', dest='solar15dayforecast_api_key',
                        required=True, help='15-day Solar Forecast API Key')
    parser.add_argument('--cleanedhistorical_api_key', action='store', dest='cleanedhistorical_api_key',
                        required=True, help='Cleaned Historical Observations API Key')

    try:
        # Now connect to the messaging system
        api = GoFlexAPI(argv, parser)
        formatter = GoFlexMessageFormatter()

        lat =  53.27
        lng = -9.05

        # Weather Service - 2-day Hourly Forecast
        msg = formatter.weatherServiceTwoDayHourlyForecast(api.args.twodayhourlyforecast_api_key, lat, lng)
        duration = api.invoke_service(msg, 1, process_result, timeout=30)
        print('Duration: ', duration)
                 
        # Weather Service - Solar 15-day Hourly Forecast
        msg = formatter.weatherServiceSolar15DayHourlyForecast(api.args.solar15dayforecast_api_key, lat, lng)
        duration = api.invoke_service(msg, 2, process_result, timeout=30)
        print('Duration: ', duration)
        
        # Weather Service - Cleaned Historical Observations
        msg = formatter.weatherServiceCleanedHistorical(api.args.cleanedhistorical_api_key, lat, lng, "2017-01-01", 1)
        duration = api.invoke_service(msg, 3, process_result, timeout=30)
        print('Duration: ', duration)
    except KeyboardInterrupt:
        print("Stopping")
    except Exception as e:
        print(e)


if __name__ == '__main__':
    main(sys.argv)



