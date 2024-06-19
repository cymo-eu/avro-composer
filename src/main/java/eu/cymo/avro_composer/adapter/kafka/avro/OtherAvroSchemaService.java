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
public class OtherAvroSchemaService {
    private final SchemaRegistryClient client;

    public OtherAvroSchemaService(SchemaRegistryClient client) {
        this.client = client;
    }

    public void register(String subject, Schema schema) {
        try {
            client.register(subject, new AvroSchema(schema));
        } catch (IOException | RestClientException e) {
            throw new RuntimeException(e);
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
    
    public int getVersion(String subject, Schema schema) {
        try {
            return client.getVersion(subject, new AvroSchema(schema));
        } catch (IOException | RestClientException e) {
            throw new RuntimeException(e);
        }
    }
}
