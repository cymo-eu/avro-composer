package eu.cymo.avro_composer.adapter.kafka.avro;

import java.io.IOException;
import java.util.Optional;

import org.apache.avro.Schema;
import org.springframework.stereotype.Component;

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

@Component
public class SchemaRegistryClientAvroSchemaService implements AvroSchemaService {
    private final SchemaRegistryClient client;
    
    public SchemaRegistryClientAvroSchemaService(
            SchemaRegistryClient client) {
        this.client = client;
    }

    public void register(String subject, Schema schema) throws RegistrationException {
        try {
            client.register(subject, new AvroSchema(schema));
        } catch (IOException | RestClientException e) {
            throw new RegistrationException("Failed to register schema for subject '%s': %s".formatted(subject, schema), e);
        }
    }

    public Schema getLatestSchema(String subject) {
        try {
            return Optional.ofNullable(client.getLatestSchemaMetadata(subject))
                    .map(SchemaMetadata::getSchema)
                    .map(new Schema.Parser()::parse)
                    .orElse(null);
        } catch (IOException | RestClientException e) {
            return null;
        }
    }

    public int getVersion(String subject, Schema schema) throws RetrieveVersionException {
        try {
            return client.getVersion(subject, new AvroSchema(schema));
        } catch (IOException | RestClientException e) {
            throw new RetrieveVersionException("Failed to retrieve version for schema '%s'".formatted(schema.getFullName()), e);
        }
    }
}
