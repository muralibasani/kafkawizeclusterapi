# Deprecated

Kafkawize has been deprecated as of September 2022. The new repository 'Klaw' (cloned from Kafkawize) is under Aiven's Git Organization, and it remains Open source.

Klaw - [GithubRepo](https://github.com/aiven/klaw)
Klaw ClusterApi - [GithubRepo](https://github.com/aiven/klaw-cluster-api)
Documentation - [Docs](https://klaw-project.io/docs)

Any further enhancements or changes will be applied to the new Klaw repo.

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