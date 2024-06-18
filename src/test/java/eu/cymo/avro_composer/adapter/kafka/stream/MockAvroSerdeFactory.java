package eu.cymo.avro_composer.adapter.kafka.stream;

import java.util.Map;

import eu.cymo.avro_composer.adapter.kafka.avro.AvroSerdeFactory;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.confluent.kafka.serializers.subject.TopicRecordNameStrategy;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;

public class MockAvroSerdeFactory implements AvroSerdeFactory {
    private SchemaRegistryClient schemaRegistryClient;
    
    public MockAvroSerdeFactory(
            SchemaRegistryClient schemaRegistryClient) {
        this.schemaRegistryClient = schemaRegistryClient;
    }

    @Override
    public GenericAvroSerde genericAvroSerde(boolean isKey) {
        var serde = new GenericAvroSerde(schemaRegistryClient);
        serde.configure(getProperties(), isKey);
        return serde;
    }
    
    private Map<String, Object> getProperties() {
        return Map.of(// property must be present, but will not be used
                KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://schema-url",
                // automatically register schemas when writing to kafka, so that the
                // SchemaRegistryClient can be updated
                KafkaAvroSerializerConfig.AUTO_REGISTER_SCHEMAS, true,
                AbstractKafkaSchemaSerDeConfig.VALUE_SUBJECT_NAME_STRATEGY, TopicRecordNameStrategy.class);
    }

}
