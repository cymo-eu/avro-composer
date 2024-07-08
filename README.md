# Avro Composer (AC)

The purpose of this project is to join schemas posted on a topic with a `TopicRecordNameStrategy` into a single avro schema and forward it as a `TopicNameStrategy`.

## Example

Consider an input topic with the following events posted there:

```idl
@namespace("eu.cymo.kafkaSerializationEvolution.event")
protocol Event {

	record OrderConfirmed {
	    string orderId;
	}

	record OrderShipped {
	    string orderId;
		  string deliveryPartner;
	}

	record OrderDelivered {
	    string orderId;
		  string deliveryPartner;
	}
}
```
The composer will create a output schema that looks like the following:
```
@namespace("eu.cymo.kafkaSerializationEvolution.event")
protocol Event {

  record Unknown {}

	record OrderEvent {
      union {
        Unknown,
        OrderConfirmed,
        OrderShipped,
        OrderDelivered
      } event;
	}
   
}
```

## Configuration

### Kafka

AC is powered by spring boot and kafka-streams, and will use `KafkaProperties` to bind your application yml, e.g:

```
spring:
  kafka:
    bootstrap-servers: ${KAFKA_BOOTSTRAP_SERVER}
    streams:
      application-id: avro-composer
      auto.offset.reset: earliest
      properties:
        processing.guarantee: exactly_once_v2
        # kafka auth
        sasl.mechanism: PLAIN
        sasl.jaas.config: org.apache.kafka.common.security.plain.PlainLoginModule required username='${KAFKA_USERNAME}' password='${KAFKA_PASSWORD}';
        security.protocol: SASL_SSL
        # schema registry
        # schema registry
        schema.registry.url: ${SCHEMA_URL}
        schema.registry.basic.auth.credentials.source: USER_INFO
        schema.registry.basic.auth.user.info: "${SCHEMA_USERNAME}:${SCHEMA_PASSWORD}"
```

### Topics

AC requires an input & output topic to be configured:

```
composer:
  topics:
    input: input
    output: output
```

### Composed Schema

The application requires some properties to be set to create the new schema for the output topic:

```
composer:
  topics:
    name: OrderEvent
    namespace: eu.cymo.kafkaSerializationEvolution.event
    documentation: |-
      Documentation to be included in the avro schema
    subject: output-value # expected value for the subject, should be ${topic}-value
```

### Tracing

Open telemetry has been included to verify the performance of the composition. By default this is enabled, but with the following configuration, it can be enabled again:

```
management:
  tracing:
    sampling:
      probability: 1.0
  zipkin:
    tracing:
      enabled: true
      endpoint: http://localhost:9411/api/v2/spans
```

## Building/running

This project contains `jib-maven-plugin` and will build to  `eu.cymo/avro-composer:${project.version}`. `mvn clean install jib:dockerBuild` will add the docker image to your local repository.

The following command will run the application with a custom application.yml file:

```
docker run \
  -v /path/to/local/application.yml:/config/application.yml \
  -e SPRING_CONFIG_LOCATION=optional:classpath:/,file:/config/application.yml \
  -p 8080:8080 \
  eu.cymo/avro-composer:latest
```

## Interals - how does the composition work

Messages from the incoming topic are compared to the latest version of the schema output topic. The schema should contain an event field that is a union of all the event types from the input topic. Depending on the schema versions, and the presence or lack there of in the output schema, different scenarios can happen.

After that the output schema is considered up to date, the input message will become the `event` property of the output message, and will be forwarded to the output topic.

### Schema for output topic does not exist

A new schema will be prepared for the output topic. It will contain a event property with as type a union of `Unknown`, which is an empty type that can be used as a fallback, and also the **latest** version of the schema of the input.

If we are using a newer schema for the value of the input topic, we need to adapt the record to match it, so some fields could be added/removed in the process.

#### Importance of using latest version of schema vs the actual version of the input event
The reason the latest version is used here, is to avoid conflicts of version withing the union of the event property.
E.g: say the union was to contain 2 events OrderDelivered and OrderShipped, both with a property Order containing an `string orderId`. If in the future that Order record were to be updated to also contain a timestamp and the composer first gets that updated through an OrderDelivered record, there will be a internal inconsistency in the output schema, because OrderShipped is using an older version of Order.

### Schema for output topic exists, but event union does not contain schema from input topic

Then the output schema will be updated and the event property union will now contain the **latest** version for the schema of the input. Again the input value will be updated to be compliant with the version used in the output schema.

### Schema for output topic exists and event union contains schema from input topic, but versions are mismatched

The value will be updated to match the **latest** version and will be forwarded in the composed schema.

### Schema for output topic exists and event union contains schema from input topic and versions match

The valeu will be forwared as the event property in the composed schema.