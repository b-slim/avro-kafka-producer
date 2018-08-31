#HOW TO:

## Build
``` shell 
mvn clean install
```
## Send data 
``` shell
java  -Dbootstrap.servers=cn105-10.l42scl.hortonworks.com:9092 -Dkafka.topic=test-avro-3 -Dkafka.num.records=600 -jar target/kafka-avro-producer.jar 
```

