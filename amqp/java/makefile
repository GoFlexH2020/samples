ifeq ($(OSTYPE),Windows_NT)
    uname_S:=Windows
else
    uname_S:=$(shell uname -s)
endif

ifeq ($(uname_S), Windows)
	DIR= 
endif
ifeq ($(uname_S), Darwin)
	DIR=/private/var/example
else
	DIR=/var/example
endif


clean:
	-mvn clean

build:
	mvn compile

test:
	mvn test

install:
	docker run --env LICENSE=accept --env MQLIGHT_USER=this_user --env MQLIGHT_PASSWORD=my_password --volume $(DIR):/var/mqlight --publish 5672:5672 --publish 9180:9180 --detach ibmcom/mqlight:1.0
