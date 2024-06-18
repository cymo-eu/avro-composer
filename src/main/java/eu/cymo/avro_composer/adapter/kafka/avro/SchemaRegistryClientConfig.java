package eu.cymo.avro_composer.adapter.kafka.avro;

import java.util.Collections;
import java.util.Map;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.ssl.SslBundles;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientFactory;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;

@Configuration
public class SchemaRegistryClientConfig {
    
    @Bean
    public SchemaRegistryClient schemaRegistryClient(
            KafkaProperties properties,
            ObjectProvider<SslBundles> sslBundles) {
        var config = new KafkaSchemaSerDeConfig(properties.buildStreamsProperties(sslBundles.getIfAvailable()));
        
        return SchemaRegistryClientFactory.newClient(
                config.getSchemaRegistryUrls(),
                config.getMaxSchemasPerSubject(),
                Collections.singletonList(new AvroSchemaProvider()),
                config.originalsWithPrefix(""),
                config.requestHeaders());
    }
    
    private class KafkaSchemaSerDeConfig extends AbstractKafkaSchemaSerDeConfig {

        public KafkaSchemaSerDeConfig(Map<?, ?> originals) {
            super(
                    baseConfigDef(),
                    originals);
        }
        
    }
    
}
