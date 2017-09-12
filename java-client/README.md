# java-client
Sample Java code for AMQP communication with Service Platform.

# Installation

Prerequisites:
* AMQP Client (Version 4.2.0)
* SLF4J (API version 1.7.25, Simple version 1.7.25)
* JSON Parser: I use the JSON Parser from [mvnrepository](http://mvnrepository.com/artifact/org.json/json/20170516 "http://mvnrepository.com/artifact/org.json/json/20170516").

Under Windows
```
cd java-client
mkdir bin
set CP=.;./bin;amqp-client-4.2.0.jar;slf4j-api-1.7.25.jar;slf4j-simple-1.7.25.jar;json.jar
javac -cp %CP% -d .\bin src\de\tud\ecast\client.java src\de\tud\ecast\GOFLEXAPI.java
```

Under Linux:
```
cd java-client
mkdir bin
export CP=.:./bin:amqp-client-4.2.0.jar:slf4j-api-1.7.25.jar:slf4j-simple-1.7.25.jar:json.jar
javac -cp %CP% -d ./bin src/de/tud/ecast/client.java src/de/tud/ecast/GOFLEXAPI.java
```

Configuration:
* I do not pass the configuration from the command line but from the file *config.properties*.

# Usage

The client class contains a main() function, so just call it. Make sure that the classpath is correct.

```
cd java-client
java -cp %CP% de.tud.ecast.client
```