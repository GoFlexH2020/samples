# Sample code for RabbitMQ messaging

The sample code is written in Python, version 2.7.12 was used for testing.
Assuming Python and pip are alreadu installed, run:

``` 
pip install -r requirements.txt
``` 


## Service Requests

A request to the GoFlex service platform takes the following form:

``` 
{
    "serviceRequest": {
        "requestor": {
            "replyTo": "the reply topic",
            "clientID": "your client id", 
            "correlationID": a number to help you identify the request
        },
        "service": {
            "args": {
               "ts_id": the meter, 
               "from": "date range begin",
               "to": "date range end" 
            },
            "name": "MeterDataRetrievalService"
        }
    }
}

``` 

## Operation

To run the client sample code:

python client.py --host=HOST --port=PORT --user=USER --password=PASSWORD --subscribe='ResponseTOPIC' --publish='RequestTOPIC'

The credentials for the host must be available prior to launch.

