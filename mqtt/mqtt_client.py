#!/usr/bin/env python
#authors john.d.sheehan@ie.ibm.com, mark_purcell@ie.ibm.com

import argparse
import csv
import json
import time
import sys
import datetime
from dateutil.tz import tzutc

from goflexsubmitapi import GoFlexMeterSubmissionAPI

#MQTT publish sample. Read a row from a csv file and publish it to an MQTT broker

def anonymize(row):
    #Funtion to anoymize a row
    #Modify as appropriate
    return row


def to_json(client, row, seperator=','):
    #split a csv row into values and insert into dictionary
    values = row.split(seperator)

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


def publish(client, filename):
    data = [];
    count = 0

    #Iterate over csv file and upload to server
    for row in read_row(filename):
        row_str = ','.join(row)

        #Call our function to remove sensitive personal data, if any
        anon = anonymize(row_str)

        #Lets convert our comma separated values to json and add to upload
        data.append(to_json(client, anon))

        count += 1
        if count % 100 == 0:
            #Now upload
            client.publish(json.dumps(data))
            data = []

    if len(data) > 0:
        print(json.dumps(data))
        client.publish(json.dumps(data))


def main(argv=None):
    parser = argparse.ArgumentParser(description='submit data to ingestion service')
    parser.add_argument('--config', action='store', dest='config',
                 required=True, help='configuration file')
    parser.add_argument('--data', action='store', dest='data',
                     required=True, help='data file')

    args = parser.parse_args()
    client = None

    try:
        #Connect to the server
        client = GoFlexMeterSubmissionAPI(args.config)

        #Now lets upload our meter data
        publish(client, args.data)
    except Exception as e:
        print 'Error: %r' % e
    finally:
        if client is not None:
            client.close()


if __name__ == '__main__':
    main(sys.argv)
