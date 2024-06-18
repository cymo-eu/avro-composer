package eu.cymo.avro_composer.adapter.kafka.avro;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.ssl.SslBundles;
import org.springframework.stereotype.Component;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;

@Component
public class ConfigurationAvroSerdeFactory implements AvroSerdeFactory {
    private final KafkaProperties kafkaProperties;
    private final ObjectProvider<SslBundles> sslBundles;
    private final SchemaRegistryClient schemaRegistryClient;

    public ConfigurationAvroSerdeFactory(
            KafkaProperties kafkaProperties,
            ObjectProvider<SslBundles> sslBundles,
            SchemaRegistryClient schemaRegistryClient) {
        this.kafkaProperties = kafkaProperties;
        this.sslBundles = sslBundles;
        this.schemaRegistryClient = schemaRegistryClient;
    }

    @Override
    public GenericAvroSerde genericAvroSerde(boolean isKey) {
        var serde = new GenericAvroSerde(schemaRegistryClient);
        serde.configure(kafkaProperties.buildStreamsProperties(sslBundles.getIfAvailable()), isKey);
        return serde;
    }

}
