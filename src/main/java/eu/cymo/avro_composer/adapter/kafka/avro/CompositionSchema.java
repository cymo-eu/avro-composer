package eu.cymo.avro_composer.adapter.kafka.avro;

import java.util.List;
import java.util.Objects;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;

public record CompositionSchema(Schema schema) {
    public static final String EVENT_FIELD = "event";

    /**
     * This method checks if an exact match of a schema is available
     */
    public boolean eventUnionContains(Schema schema) {
        return eventFieldUnionTypes().contains(schema);
    }
    
    /**
     * This method checks if a schema with the same name & namespace is
     * available, this could mean though that the 2 schemas are different
     * versions.
     * 'eventUnionContains' checks if the content of the schema is the
     * same as well.
     */
    public boolean eventUnionHasSchemaWithNameAndNamespace(Schema schema) {
        return eventFieldUnionTypes().stream()
                .anyMatch(s -> hasSameNameAndNameSpace(s, schema));
    }
    
    public Schema getSchemaWithNameAndNameSpace(Schema schema) {
        return eventFieldUnionTypes().stream()
                .filter(s -> hasSameNameAndNameSpace(s, schema))
                .findFirst()
                .get();
    }
    
    private boolean hasSameNameAndNameSpace(Schema left, Schema right) {
        return Objects.equals(left.getName(), right.getName()) &&
                Objects.equals(left.getNamespace(), right.getNamespace());
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
