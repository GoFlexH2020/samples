#!/usr/bin/env python
#author mark_purcell@ie.ibm.com

import pika
import uuid
import json
import ssl

#NOTE: FOR GOFLEX OPERATIONS DONT CHANGE THE CONTENTS OF THIS FILE
#REQUEST BUG FIXES OR ENHANCEMENTS AS NECESSARY


class GoFlexAPI():
    def __init__(self, host, port, user, password, vhost, cert, publish, subscribe):
        self.client_id = str(uuid.uuid4())
        self.publish_topic = publish
        self.subscribe_topic = subscribe + '/' + self.client_id

        self.publisher = self.connect(host, port, user, password, vhost, cert)
        self.publisher.queue_declare(queue=self.publish_topic, durable=True)
        self.subscriber = self.connect(host, port, user, password, vhost, cert)
        self.subscriber.queue_declare(queue=self.subscribe_topic, auto_delete=True)

    def connect(self, host, port, user, password, vhost, cert):
        ssl_options = dict(ssl_version=ssl.PROTOCOL_TLSv1_2,
                            ca_certs=cert,
                            cert_reqs=ssl.CERT_REQUIRED)
        credentials = pika.PlainCredentials(user, password)
        parameters = pika.ConnectionParameters(host, port, vhost, credentials, ssl=True, ssl_options=ssl_options)
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
            if msg is None:
                break

            messages += 1 
            method_frame, properties, body = msg
            self.subscriber.basic_ack(method_frame.delivery_tag)

            fullmessage = json.loads(body)
            service = fullmessage['serviceResponse']['service']
            code = service['status']
            correlation = fullmessage['serviceResponse']['requestor']['correlationID']

            done = message_handler(fullmessage, service, code, correlation)
            if done is not None:
                break;

        return messages

