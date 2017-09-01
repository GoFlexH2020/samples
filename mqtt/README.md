### Code sample

Sample mqtt client. Reads the contents of `./sample.csv` and publishes each line to an MQTT broker, as specified via a configuration file.

#### Requirements
```
Python 2.7.12
pip 9.0.1
```

Install package dependencies
```
pip install -r requirements.txt
```

####
Run 

```
python mqtt_client.py --config=a config file
```


