# Beam CDC Prototype

### Running the Beam SDF prototype to consume Kafka Connect Sources

The utilities defined in `KafkaSourceConsumerFn` are meant to consume data from
implementations of Kafka `ConnectSource`s. There are tests defined in
`CounterSourceConnectorTest`, which one can run easily.

**This readme contains instructions to run tests from the command line**, but
tests can also be ran from an IDE/debugger, and they may be easier to study that
way.

#### Running `testQuickCount`

The `testQuickCount` method in `CounterSourceConnectorTest` is a test of a
basic implementation of a Kafka `ConnectSource`. This test simply verifies
that a Beam pipeline consuming a count from 1 to 10 will run successfully.

Run this test:

```shell script
./gradlew :sdks:java:io:cdc:test --tests=**testQuickCount -xlint
```

#### Running `testDebeziumConnector`

The `testDebeziumConnector` method actually runs based on the [Debezium
 quickstart](https://debezium.io/documentation/reference/1.3/tutorial.html). This
means that it depends on having a MySQL database running.

First we [start the MySQL database](
https://debezium.io/documentation/reference/1.3/tutorial.html#starting-mysql-database)
using docker:
```shell script
$ docker run -it --rm --name mysql -p 3306:3306 -e MYSQL_ROOT_PASSWORD=debezium -e MYSQL_USER=mysqluser -e MYSQL_PASSWORD=mysqlpw debezium/example-mysql:1.3
```

Once the database is started, we can run the test. **Note** that this test
**will not finish**, and will continuously generate output, so after running for
some time, you should interrupt it:
```shell script
./gradlew :sdks:java:io:cdc:test --tests=**testDebeziumConnector -xlint
```
