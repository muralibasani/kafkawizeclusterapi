# Kafkawize

Kafkawize is a Self service Apache Kafka Topic Management tool/portal. It is a web application which automates the process of creating and browsing Kafka topics, acls, avro schemas, connectors by introducing roles/authorizations to users of various teams of an organization.

KafkaWize ClusterApi application.  It is required to run KafkaWize https://github.com/muralibasani/kafkawize before running this api.

This cluster api communicates with Kafka brokers with AdminClientApi and connects to UserInterfaceApi (kafkawize).

[![Features](https://yt-embed.herokuapp.com/embed?v=i7nmi-lovgA)](https://www.youtube.com/watch?v=i7nmi-lovgA "Create a kafka topic")

## Built With

* [Maven](https://maven.apache.org/) - Dependency Management
* Java, Spring boot, Spring security, Kafka Admin client

## Versioning

For the versions available, see the [tags on this repository](https://github.com/muralibasani/kafkawizeclusterapi/tags).

## Authors

* **Muralidhar Basani** - [muralibasani](https://github.com/muralibasani)

## License

This project is licensed under the Apache License  - see the [LICENSE.md](LICENSE.md) file for details.

## Architecture:

![Architecture](https://github.com/muralibasani/kafkawize/blob/master/screenshots/arch.png)

## Install

mvn clean install
Follow steps at https://kafkawize.readthedocs.io/en/latest