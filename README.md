# Kinesis Flink with JDBC sink

Rabbit -> Flink app -> AWS Mysql

## Get started

### Package

```
mvn clean compile && mvn package -Prabbit-source-ticker
```

### Run in Flink local cluster

```
flink run target/kinesis_flink_jdbc-0.4.3-rabbit-source-ticker.jar <<list of config>>
```

### Run in AWS Kinesis Analytic

Follow AWS guide.