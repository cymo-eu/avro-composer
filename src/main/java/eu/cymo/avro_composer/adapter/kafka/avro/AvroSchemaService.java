package eu.cymo.avro_composer.adapter.kafka.avro;

import org.apache.avro.Schema;

public interface AvroSchemaService {

    public void register(String subject, Schema schema) throws RegistrationException;

    public Schema getLatestSchema(String subject);

    public int getVersion(String subject, Schema schema) throws RetrieveVersionException;
    
}
