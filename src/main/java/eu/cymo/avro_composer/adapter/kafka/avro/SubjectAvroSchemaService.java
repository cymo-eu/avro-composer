package eu.cymo.avro_composer.adapter.kafka.avro;

import java.io.IOException;
import java.util.Optional;

import org.apache.avro.Schema;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import eu.cymo.avro_composer.adapter.kafka.stream.CompositionConfig;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

@Component
public class SubjectAvroSchemaService {
    private final SchemaRegistryClient client;
    private final CompositionConfig config;
    
    private CompositionSchema cachedSchema;
    
    @Autowired
    public SubjectAvroSchemaService(
            SchemaRegistryClient client,
            CompositionConfig config) {
        this.client = client;
        this.config = config;
    }
    
    public CompositionSchema register(CompositionSchema schema) throws RegistrationException {
        try {
            client.register(config.getSubject(), schema.toParsedSchema());
            setCachedSchema(schema);
            return schema;
        } catch (IOException | RestClientException e) {
            setCachedSchema(null);
            throw new RegistrationException("Failed to register schema '%s' for subject '%s'".formatted(schema.schema(), config.getSubject()), e);
        }
    }
    
    public Optional<CompositionSchema> getLatestSchema() throws RetrieveMetadataException {
        var cached = getCachedSchema();
        if(cached.isPresent()) {
            return cached;
        }
        return getLatestSchemaMetadata()
                .map(SchemaMetadata::getSchema)
                .map(new Schema.Parser()::parse)
                .map(CompositionSchema::new);
    }
    
    private Optional<SchemaMetadata> getLatestSchemaMetadata() throws RetrieveMetadataException {
        try {
            return Optional.ofNullable(client.getLatestSchemaMetadata(config.getSubject()));
        } catch (RestClientException e) {
            if(e.getStatus() == 404) {
                return Optional.empty();
            }
            throw failedToRetrieveMetadataException(e);
        }
        catch (IOException e) {
            throw failedToRetrieveMetadataException(e);
        }
    }
    
    private RetrieveMetadataException failedToRetrieveMetadataException(Throwable e) {
        return new RetrieveMetadataException("Failed to retrieve schema metadata for '%s'".formatted(config.getSubject()), e);
    }
    
    private void setCachedSchema(CompositionSchema schema) {
        this.cachedSchema = schema;
    }
    
    private Optional<CompositionSchema> getCachedSchema() {
        return Optional.ofNullable(cachedSchema);
    }
}
