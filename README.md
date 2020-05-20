### Spark Kafka Sample

Spark Kafka example for CDP-DC 7.0.3 (Kafka 2.3.0, Spark 2.4.0)
```
mvn clean package
spark-submit --class com.cloudera.fce.curtis.spark_kafka_sample.SparkKafka target/spark_kafka_sample-1.0-jar-with-dependencies.jar <KAFKA_BROKER_HOST>:9092 <TOPIC_NAME> 
```
