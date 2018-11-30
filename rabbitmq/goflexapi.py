#!/usr/bin/env python
#author mark_purcell@ie.ibm.com

import argparse
import pika
import uuid
import json
import ssl
import timeit

#NOTE: FOR GOFLEX OPERATIONS DONT CHANGE THE CONTENTS OF THIS FILE
#REQUEST BUG FIXES OR ENHANCEMENTS AS NECESSARY


class GoFlexAPI():
    def __init__(self, argv=None, parser=None):
        if argv == None:
            return

        if parser == None:
            parser = argparse.ArgumentParser(description='GoFlex client')

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
        parser.add_argument('--iterations', action='store', dest='iterations',
                 required=False, help='Loop counter', default=1, type=int)

        #Specifies a shared or private subscribe topic
        parser.add_argument('--private', dest='private', action='store_true')
        parser.add_argument('--shared', dest='private', action='store_false')
        parser.set_defaults(private=True)

        args = parser.parse_args()
        self.args = args

        self.init(args.host, args.port, args.user, args.password, args.vhost, 
                          args.cert, args.publish_topic, args.subscribe_topic, args.private)

    def init(self, host, port, user, password, vhost, cert, publish, subscribe, private):
        self.publish_topic = publish
        self.client_id = str(uuid.uuid4())
        self.subscribe_topic = subscribe

        if private == True:
            self.subscribe_topic += '/' + self.client_id

        self.publisher = self.connect(host, port, user, password, vhost, cert)
        self.publisher.queue_declare(queue=self.publish_topic, durable=True)
        self.subscriber = self.connect(host, port, user, password, vhost, cert)
        self.subscriber.queue_declare(queue=self.subscribe_topic, durable=True, auto_delete=private)

    def connect(self, host, port, user, password, vhost, cert):
        ssl_options = dict(ssl_version=ssl.PROTOCOL_TLSv1_2,
                            ca_certs=cert,
                            cert_reqs=ssl.CERT_REQUIRED)
        credentials = pika.PlainCredentials(user, password)
        parameters = pika.ConnectionParameters(host, port, vhost, credentials, 
                                ssl=True, ssl_options=ssl_options,
                                connection_attempts=10,
                                retry_delay=1)
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()
        return channel

    def publish(self, message, correlation):
        message['serviceRequest']['requestor'] = {}
        message['serviceRequest']['requestor']['replyTo'] = self.subscribe_topic
        message['serviceRequest']['requestor']['clientID'] = self.client_id
        message['serviceRequest']['requestor']['transient'] = True
        message['serviceRequest']['requestor']['correlationID'] = correlation
        json_message = json.dumps(message)

        var = self.publisher.basic_publish(exchange='', routing_key=self.publish_topic, body=json_message,
                            properties=pika.BasicProperties(
                                delivery_mode = 1,
                          ))

    def receive(self, timeout, message_handler):
        messages = 0

        for msg in self.subscriber.consume(self.subscribe_topic, inactivity_timeout=timeout):
            if not msg: 
                break

            method_frame, properties, body = msg
            if not method_frame:
                break

            messages += 1 
            self.subscriber.basic_ack(method_frame.delivery_tag)

            fullmessage = json.loads(body)
            service = fullmessage['serviceResponse']['service']
            code = service['status']
            correlation = fullmessage['serviceResponse']['requestor']['correlationID'] if 'requestor' in 'serviceResponse' else None

            done = message_handler(fullmessage, service, code, correlation)
            if done is not None:
                break;

        return messages

    def invoke_service(self, message, correlation, callback, timeout=30):
        self.publish(message, correlation)

        #Now wait for the reply
        start_time = timeit.default_timer()
        messages = self.receive(timeout, callback)
        if messages == 0:
            raise Exception('Timed out waiting for reply.')
        return timeit.default_timer() - start_time


