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


#MQTT publish sample. Read a row from a csv file and publish it to an MQTT broker


_logger = logging.getLogger(__package__)

def logger(verbose=False):
    global _logger

    if verbose:
        level = logging.DEBUG
    else:
        level = logging.INFO

    if not _logger.handlers:
        logging.basicConfig(stream=sys.stdout, level=level,
            format='%(asctime)s.%(msecs)03d %(levelname)-6s %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S')
    return _logger



class DataParser(object):
    def __init__(self, client, flavour, batch, max_lines):
        self.client = client
        self.flavour = flavour
        self.batch = batch
        self.max_lines = max_lines
        self.header = None
        self.finished = False
        self.start = None
        self.device = None
        self.last_processed_line = 0
        
        #Update your date format as appropriate
        #This date format matches the sample.csv file
        self.date_format = '%Y-%m-%d %H:%M:%S'

        #If your date format is different, please change, for example
        #01/09/2015 00:00 requires the following change
        #self.date_format = '%d/%m/%Y %H:%M'

        #Update your timezone as appropriate
        self.timezone = 'Europe/Nicosia'
        #If your timestamp is already UTC the following line is appropriate
        #self.timezone = 'UTC'
        #self.timezone = 'Europe/Zurich'


    def anonymize(self, row):
        #Funtion to anoymize a row
        #Modify as appropriate
        return row


    def read_row(self, filename):
        #Open csv file and yield a new row on each call

        with open(filename, 'r') as csvfile:
            csvreader = csv.reader((line.replace('\0','') for line in csvfile), delimiter=',', quotechar='"')

            for row in csvreader:
                yield row


    def parse_sample_line(self, row, line, target_line, separator=','):
        if (line <= target_line):
            return

        #split a csv row into values and insert into dictionary
        values = row.split(separator)

        #Call our function to remove sensitive personal data, if any
        values = self.anonymize(values)

        #Note: the observed_timestamp field should be iso8601 UTC prior to submission
        #The following code assumes a local timestamp and converts to UTC

        timestamp = self.client.utc_offset(values[1], self.timezone, self.date_format)
        timestamp = datetime.datetime.strptime(timestamp, self.date_format).replace(tzinfo=tzutc()).isoformat()

        data = []
        data.append({"observed_timestamp": timestamp, "device_id": values[0], "value": values[2]})
        self.last_processed_line = line
        return data


    def publish(self, filename, target_line):
        count = 0
        line = 0
        data = []

        #Iterate over csv file and upload to server
        for row in self.read_row(filename):
            line += 1
            row_str = ','.join(row)

            #Lets convert our comma separated values to json and add to upload
            latest = self.parse_sample_line(row_str, line, target_line)
            if latest is None:
                continue

            data = data + latest

            count += 1
            if count % self.batch == 0:
                #Now upload
                logger().debug("Publishing : %d (%d measurements)" % (count, len(data)))
                self.client.publish(json.dumps(data))
                #logger().info(json.dumps(data))
                data = []

            if count >= self.max_lines:
                break

        if (data is not None) and (len(data) > 0):
            logger().debug("Publishing : %d (%d measurements)" % (count, len(data)))
            self.client.publish(json.dumps(data))
            #logger().info(json.dumps(data))

        return (self.last_processed_line, count)


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
    parser.add_argument('--batch', action='store', dest='batch',
                    required=True, help='batch x messages')
    parser.add_argument('--max', action='store', dest='max_lines',
                    required=False, default=100000, help='process max lines')
    parser.add_argument('-v', '--verbose', help="increase output verbosity",
                    required=False, default=False, action='store_true', dest='verbose')

    args = parser.parse_args()
    client = None
    state = {}

    logger(args.verbose)
    logger().info("=============================Starting==============================")

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
            fname = os.path.join(args.dir,fname)
            if os.path.isfile(fname) == 0:
                continue

            if fname < state['file']:
                logger().debug("Skipping file: %s" % fname)
                continue
            elif fname == state['file']:
                line = state['line'] #Lets try to start from the last point
                logger().info("Processing from : %s:%d" % (fname, line))
            else:
                line = 0 #A new file, start at the beginning
                logger().info("Processing : %s..." % fname)

            try:
                #Now lets upload our meter data
                dp = DataParser(client, int(args.flavour), int(args.batch), int(args.max_lines))
                line, count = dp.publish(fname, line)
                if (line == 0) or (line == state['line']):
                    logger().info("No additional data at : %s:%d" % (fname, state['line']))
                    state['line'] = 0 #Now we drop state to process next file
                    continue

                logger().info("Processed : %s" % (fname))

                with open(args.state, 'w') as state_file:
                    json.dump({'file': fname, 'line': line}, state_file)

                if count == dp.max_lines:
                    #Quit all file processing, large file encountered
                    logger().info("Max lines reached : %s:%d:%d" % (fname, line, count))
                    break
            except Exception as e:
                logger().info("WARNING: %r" % e)
                logger().info("WARNING: Not writing to state file : %s" % args.state)
                logger().info("WARNING: State will not be preserved : %s:%d" % (fname, line))
                raise(e)
    except Exception as e:
        logger().error("ERROR: %r" % e)
        logger().error("ERROR: aborting.")
    finally:
        if client is not None:
            logger().debug("Disconnecting from broker...")
            client.close()
        logger().info("=============================Finished==============================")


if __name__ == '__main__':
    main(sys.argv)

