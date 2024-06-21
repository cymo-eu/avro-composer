package eu.cymo.avro_composer.adapter.kafka.stream.topology;

import org.springframework.context.annotation.Bean;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;

public class MockSchemaRegistryClientConfig {

    @Bean
    public SchemaRegistryClient schemaRegistryClient() {
        return MockSchemaRegistry.getClientForScope("test-scope");
    }
    
}
