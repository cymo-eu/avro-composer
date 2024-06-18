package eu.cymo.avro_composer.adapter.kafka.avro;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.Optional;

import org.apache.avro.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import eu.cymo.avro_composer.adapter.kafka.stream.CompositionConfig;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;

class SubjectAvroSchemaServiceTest {
    private final String subject = "subject";
    
    private SchemaRegistryClient schemaRegistryClient;
    
    private SubjectAvroSchemaService service;
    
    @BeforeEach
    void setup() {
        schemaRegistryClient = MockSchemaRegistry.getClientForScope("test-scope");
        
        var config = new CompositionConfig();
        config.setSubject(subject);
        
        service = new SubjectAvroSchemaService(
                schemaRegistryClient,
                config);
    }
    
    @AfterEach
    void breakDown() throws IOException, RestClientException {
        schemaRegistryClient.deleteSubject(subject);
    }
    
    @Nested
    class Register {
        
        @Test
        void registersToSchemaRegistry() throws IOException, RestClientException, RegistrationException {
            // given
            var schema = new CompositionSchema(Schema.create(Schema.Type.STRING));
            
            // when
            service.register(schema);
            
            // then
            var subjectSchema = Optional.ofNullable(schemaRegistryClient.getLatestSchemaMetadata(subject))
                    .map(SchemaMetadata::getSchema)
                    .map(new Schema.Parser()::parse)
                    .orElseThrow(() -> new RuntimeException("Failed to retrieve latest schema metdata for '%s'".formatted(subject)));
            assertThat(schema.schema()).isEqualTo(subjectSchema);
        }
        
    }
    
    @Nested
    class GetLatestSchema {
        
        @Test
        void isEmpty_whenNoValueForSchema() throws RetrieveMetadataException {
            // when
            var result = service.getLatestSchema();
            
            assertThat(result).isEmpty();
        }
        
        @Test
        void retrievesLatestSchemaForSubject() throws IOException, RestClientException, RetrieveMetadataException {
            // given
            var schema = Schema.create(Schema.Type.STRING);
            schemaRegistryClient.register(subject, new AvroSchema(schema));
            
            // when
            var result = service.getLatestSchema();
            
            // then
            assertThat(result).map(CompositionSchema::schema).hasValue(schema);
        }
        
    }
}
