#!/usr/bin/env python
#authors john.d.sheehan@ie.ibm.com, mark_purcell@ie.ibm.com

import argparse
import os
import fnmatch
import csv
import json
import time
import sys
import datetime
import logging
from dateutil.tz import tzutc
from goflexsubmitapi import GoFlexMeterSubmissionAPI


_logger = logging.getLogger()

def logger():
    global _logger

    if not _logger.handlers:
        logging.basicConfig(level=logging.INFO,
            format='%(asctime)s.%(msecs)03d %(levelname)-6s %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S')
    return _logger

#MQTT publish sample. Read a row from a csv file and publish it to an MQTT broker

def anonymize(row):
    #Funtion to anoymize a row
    #Modify as appropriate
    return row


def to_json(client, flavour, row, seperator=','):
    #split a csv row into values and insert into dictionary
    values = row.split(seperator)

    #Call our function to remove sensitive personal data, if any
    values = anonymize(values)

    #This date_format matches the format in the associated sample.csv
    date_format = '%Y-%m-%d %H:%M:%S'

    #If your date format is different, please change, for example
    #01/09/2015 00:00 requires the following change
    #date_format = '%d/%m/%Y %H:%M'

    #Note: the observed_timestamp field should be iso8601 UTC prior to submission
    #The following code assumes a local timestamp and converts to UTC

    #Update your timezone as appropriate
    #If your timestamp is already UTC the following line is appropriate
    #timezone = 'UTC'
    #timezone = 'Europe/Zurich'
    timezone = 'Europe/Nicosia'

    timestamp = client.utc_offset(values[1], timezone, date_format)
    timestamp = datetime.datetime.strptime(timestamp, date_format).replace(tzinfo=tzutc()).isoformat()

    return {"observed_timestamp": timestamp, "device_id": values[0], "value": values[2]}


def read_row(filename):
    #Open csv file and yield a new row on each call

    with open(filename, 'rb') as csvfile:
        csvreader = csv.reader(csvfile, delimiter=',', quotechar='"')
        for row in csvreader:
            yield row


def publish(client, flavour, filename, target_line):
    data = [];
    count = 0
    line = 0

    #Iterate over csv file and upload to server
    for row in read_row(filename):
        line += 1
        if line <= target_line:
            continue

        logger().info("Line " + str(line))
        row_str = ','.join(row)

        #Lets convert our comma separated values to json and add to upload
        data.append(to_json(client, flavour, row_str))

        count += 1
        if count % 100 == 0:
            #Now upload
            client.publish(json.dumps(data))
            data = []

    if len(data) > 0:
        logger().info(json.dumps(data))
        client.publish(json.dumps(data))

    return line


def sort_key(x):
    return x.lower()


def main(argv=None):
    parser = argparse.ArgumentParser(description='submit data to ingestion service')
    parser.add_argument('--broker', action='store', dest='broker',
                 required=True, help='broker configuration file')
    parser.add_argument('--dir', action='store', dest='dir',
                     required=True, help='data directory')
    parser.add_argument('--pattern', action='store', dest='pattern',
                     required=True, help='file filter')
    parser.add_argument('--state', action='store', dest='state',
                     required=True, help='state file')
    parser.add_argument('--flavour', action='store', dest='flavour',
                     required=True, help='file format style')

    args = parser.parse_args()
    client = None
    state = {}

    logger().info("Starting...")

    try:
        #Connect to the broker
        client = GoFlexMeterSubmissionAPI(args.broker)
    except Exception as e:
        logger().error("CRITICAL: Cannot connect to broker : %r" % e)
        logger().error("CRITICAL: Please check network/firewall... exiting.")
        quit()

    try:
        with open(args.state) as state_file:
            state = json.load(state_file)
            #state = open(os.path.join('.', 'state.csv'), 'r')
            logger().info("Loading from state : %s:%d" % (state['file'], state['line']))
    except:
        logger().info("No state file found, providing default.")
        state['file'] = None
        state['line'] = 0

    try:
        for fname in fnmatch.filter(sorted(os.listdir(args.dir), key = sort_key), args.pattern):
            if os.path.isfile(fname) == 0:
                continue

            if fname < state['file']:
                logger().info("Skipping file: %s" % fname)
                continue
            elif fname == state['file']:
                line = state['line'] #Lets try to start from the last point
                logger().info("Processing from : %s:%d" % (fname, line))
            else:
                line = 0 #A new file, start at the beginning
                logger().info("Processing : %s..." % fname)

            #Now lets upload our meter data
            line = publish(client, args.flavour, fname, line)
            if line == state['line']:
                logger().info("No additonal data at : %s:%d" % (fname, line))
                state['line'] = 0 #Now we drop state to process next file
                continue

            logger().info("Processed : %s" % (fname))

            try:
                with open(args.state, 'w') as state_file:
                    json.dump({'file': fname, 'line': line}, state_file)
            except Exception as e:
                logger().info("WARNING: %r" % e)
                logger().info("WARNING: Cannot write to state file : %s" % args.state)
                logger().info("WARNING: State will not be preserved : %s:%d" % (fname, line))
    except Exception as e:
        logger().error("ERROR: %r" % e)
        logger().error("ERROR: aborting.")
    finally:
        if client is not None:
            logger().info("Disconnecting from broker...")
            client.close()
        logger().info("Finished.")


if __name__ == '__main__':
    main(sys.argv)

