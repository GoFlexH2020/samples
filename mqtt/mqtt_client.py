#!/usr/bin/env python
#authors john.d.sheehan@ie.ibm.com, mark_purcell@ie.ibm.com

import argparse
import csv
import json
import time
import sys

from goflexsubmitapi import GoFlexMeterSubmissionAPI

#MQTT publish sample. Read a row from a csv file and publish it to an MQTT broker

def anonymize(row):
    #Funtion to anoymize a row
    #Modify as appropriate
    return row


def to_json(row, seperator=','):
    #split a csv row into values and insert into dictionary
    values = row.split(seperator)

    tmp = {}
    tmp['ts_id'] = values[0]
    tmp['observed_timestamp'] = values[1]
    tmp['value'] = values[2]

    return json.dumps({"msRequest": {"args": tmp}})


def read_row(filename):
    #Open csv file and yield a new row on each call
    with open(filename, 'rb') as csvfile:
        csvreader = csv.reader(csvfile, delimiter=',', quotechar='"')
        for row in csvreader:
            yield row


def publish(client, filename):
    #Iterate over csv file and upload to server
    for row in read_row(filename):
        row_str = ','.join(row)

        #Call our function to remove sensitive personal data, if any
        anon = anonymize(row_str)

        #Lets convert our comma separated values to json and upload
        data = to_json(anon)
        client.publish(data)


def main(argv=None):
    parser = argparse.ArgumentParser(description='submit data to ingestion service')
    parser.add_argument('--config', action='store', dest='config',
                 required=True, help='configuration file')

    args = parser.parse_args()
    client = None

    try:
        #Connect to the server
        client = GoFlexMeterSubmissionAPI(args.config)

        #Now lets upload our meter data
        publish(client, './sample.csv')
    except Exception as e:
        print 'Error: %r' % e
    finally:
        if client is not None:
            client.close()


if __name__ == '__main__':
    main(sys.argv)
