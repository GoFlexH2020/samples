# Communication with ECAST

This sample code presents the communication of ECAST with the Service Platform.

## Data Formats
The current data formats are shown in the following figure below:![ECAST-Communication-v0.1.png](https://raw.githubusercontent.com/GoFlexH2020/samples/ecast/ecast/ECAST-Communication-v0.1.png)

## Benchmark Request
ECAST subscribes to benchmark requests at routing key *ECAST/v0.1/Response*. A benchmark request is a JSON object that consists of:

- A *deviceId* that corresponds to a device from the MeterDataRetrievalService,
- A *startTime* as ISO-8601 date-time string without nanoseconds and with "+00:00" offset time zone (UTC) such as, e.g., "2007-12-03T10:15:30+00:00",
- An *endTime* is currently set to the current time instance,
- A *horizon* as integer, representing the length of the forecast period.

A sample JSON file can be found [here](https://raw.githubusercontent.com/GoFlexH2020/samples/ecast/ecast/BenchmarkRequest.json).

## Benchmark Response 
ECAST publishes a benchmark response to the *KeyValueService*. A benchmark response consists of:

* A key such as "ecast-device_id-\<deviceId\>"
* A value which is a String dump of a JSON that consists of:
	* A *deviceId* that corresponds to a device from the MeterDataRetrievalService,
	* A *startTime* as ISO-8601 date-time string without nanoseconds and with "+00:00" offset time zone (UTC) such as, e.g., "2007-12-03T10:15:30+00:00",
	* An *endTime* as ISO-8601 date-time string without nanoseconds and with "+00:00" offset time zone (UTC) such as, e.g., "2007-12-03T10:15:30+00:00",
	* A *horizon* as integer, representing the length of the forecast period.
	* An array of results in increasing order of their forecast error:
		* An *algorithm* that is used as forecast technique, represented by its name
		* A set of *parameters* of the algorithm represented by a string
		* The forecast error as MAPE represented by a double value
		* The run-time in milliseconds for modelling and forecasting

A sample key-value pair is the following:
`[u'ecast-device_id-7440', u'{"horizon":12,"startTime":"2010-06-01T00:00:00+00:00","endTime":"2011-06-01T00:00:00+00:00","deviceId":"7440","forecastResults":[{"error":0.388004018922359,"runtime":118,"parameters":"","algorithm":"HoltWinters"}]}']`
