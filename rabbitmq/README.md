# Sample code for RabbitMQ messaging

The sample code is written in Python, version 2.7.12 was used for testing.
Assuming Python and pip are already installed, run:

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
               "device_id": the meter, 
               "from": "date range begin",
               "to": "date range end" 
            },
            "name": "MeterDataRetrievalService"
        }
    }
}

``` 

## Operation

The sample assumes a TLS enabled RabbitMQ host. To run the client sample code:

``` 
python client.py --host=HOST --port=PORT --user=USER --password=PASSWORD --vhost=VHOST --cert=CERTFILE --subscribe='ResponseTOPIC' --publish='RequestTOPIC'
``` 

``` 
python KeyValueService-Sample.py --host=HOST --port=PORT --user=USER --password=PASSWORD --vhost=VHOST --cert=CERTFILE --subscribe='ResponseTOPIC' --publish='RequestTOPIC'
``` 
```
python WeatherService-Sample.py --host=HOST --port=PORT --user=USER --password=PASSWORD --vhost=VHOST --cert=CERTFILE --subscribe='ResponseTOPIC' --publish='RequestTOPIC' --twodayhourlyforecast_api_key YOUR_KEY --solar15dayforecast_api_key YOUR_KEY --cleanedhistorical_api_key YOUR_KEY
```

The credentials for the host must be available prior to launch.

