package eu.cymo.avro_composer.adapter.kafka.avro;

import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;

public record CompositionSchema(Schema schema) {
    public static final String EVENT_FIELD = "event";

    public boolean eventUnionContains(Schema schema) {
        return eventFieldUnionTypes().contains(schema);
    }
    
    public Field eventField() {
        return schema.getField(EVENT_FIELD);
    }
    
    public Schema eventFieldSchema() {
        return eventField().schema();
    }
    
    public List<Schema> eventFieldUnionTypes() {
        return eventFieldSchema().getTypes();
    }
    
    public ParsedSchema toParsedSchema() {
        return new AvroSchema(schema);
    }
    
}
