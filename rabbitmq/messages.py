#!/usr/bin/env python
#author mark_purcell@ie.ibm.com


#NOTE: FOR GOFLEX OPERATIONS DONT CHANGE THE CONTENTS OF THIS FILE
#REQUEST BUG FIXES OR ENHANCEMENTS AS NECESSARY


class GoFlexMessageFormatter():
    def __init__(self):
        pass

    def request_meter_data(self, meter, from_date, to_date):
        return { 
            "serviceRequest": { 
                "service": {
                 "name": "TimeseriesService",
                    "args": {
                        "cmd": "ts/get_timeseries_values",
                        "device_id": meter,
                        "from": from_date,
                        "to": to_date
                    }
                }
            }
        }

    def request_meter_list(self):
        return { 
            "serviceRequest": { 
                "service": { 
                    "name": "TimeseriesService",
                    "args": {
                        "cmd": "ts/get_time_series"
                    }
                }
            }
        }

    def store_time_series(self, values):
        return {
            "serviceRequest": {
                "service": {
                    "name": "TimeseriesService",
                    "args": {
                        "cmd": "ts/store_timeseries_values",
                        "values": values
                    }
                }
            }
        }

    def average_time_series(self, meter, from_date, to_date):
        return {
            "serviceRequest": {
                "service": {
                 "name": "TimeseriesService",
                    "args": {
                        "cmd": "ts/average_timeseries_values",
                        "device_id": meter,
                        "from": from_date,
                        "to": to_date
                    }
                }
            }
        }

    def register_model(self, model_name, entity_name, signal_name):
        return {
            "serviceRequest": {
                "service": {
                    "name": "TimeseriesService",
                    "args": {
                        "cmd": "register_model",
                        "model_name": model_name,
                        "entity": entity_name,
                        "signal": signal_name
                    }
                }
            }
        }

    def request_model_time_series(self, model_name, entity_name, signal_name):
        return {
            "serviceRequest": {
                "service": {
                    "name": "TimeseriesService",
                    "args": {
                        "cmd": "get_model_timeseries",
                        "model_name": model_name,
                        "entity": entity_name,
                        "signal": signal_name
                    }
                }
            }
        }

    def keyValueService(self, cmd, keys):
        return {
            "serviceRequest": {
                "service": {
                    "name": "KeyValueService",
                    "args": {
                        "cmd": cmd,
                        "keys": keys
                    }
                }
            }
        }

    def weatherServiceTwoDayHourlyForecast(self, api_key, lat, lng):
        return {
            "serviceRequest": { 
                "service"  : {
                    "name" : "WeatherService-TwoDayHourlyForecast-External",
                    "args" : {
                        "apiKey"    : api_key,
                        "latitude"  : lat,
                        "longitude" : lng
                    }
                }
            }
        }

    def weatherServiceSolar15DayHourlyForecast(self, api_key, lat, lng):
        return { 
            "serviceRequest": { 
                "service"  : {
                    "name" : "WeatherService-Solar15DayHourlyForecast-External",
                    "args" : {
                        "apiKey"    : api_key,
                        "latitude"  : lat,
                        "longitude" : lng
                    }
                }
            }
        }

    def weatherServiceCleanedHistorical(self, api_key, lat, lng, start, count):
        return { 
            "serviceRequest": { 
                "service"  : {
                    "name" : "WeatherService-CleanedHistorical-External",
                    "args" : {
                        "apiKey"    : api_key,
                        "latitude"  : lat,
                        "longitude" : lng,
                        "startDate" : start,
                        "numDays"   : count
                    }
                }
            }
        }


