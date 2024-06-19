package eu.cymo.avro_composer.adapter.kafka.avro;

import org.apache.avro.Schema;
import org.springframework.stereotype.Component;

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;

@Component
public class SchemaVersionService {
    private SchemaRegistryClient client;
    
    public SchemaVersionService(
            SchemaRegistryClient client) {
        this.client = client;
    }
    
    public Schema getNewerSchema(String subject, Schema left, Schema right) throws RetrieveVersionException {
        var versionLeft = getVersion(subject, left);
        var versionRight = getVersion(subject, right);
        
        return versionLeft > versionRight ? left : right;
    }
    
    public int getVersion(String subject, Schema schema) throws RetrieveVersionException {
        try {
            return client.getVersion(subject, new AvroSchema(schema));
        } catch (Exception e) {
            throw new RetrieveVersionException("Failed to retrieve version for subject '%s' and schema '%s'".formatted(subject, schema), e);
        }
    }
    
}
