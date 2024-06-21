package eu.cymo.avro_composer.adapter.kafka.avro;

import java.util.function.Predicate;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;

public abstract class GenericRecordBuilder {

    protected Schema getChildSchema(Schema schema, String field) {
        var fieldSchema = schema.getField(field).schema();
        if(fieldSchema.getType() == Type.UNION) {
            return fieldSchema.getTypes()
                    .stream()
                    .filter(Predicate.not(s -> s.getType() == Type.NULL))
                    .findFirst()
                    .orElseThrow(() -> new IllegalArgumentException("No other schemas found in UNION besides a NULL schema for field '%s'".formatted(field)));
        }
        return fieldSchema;
    }
    
}
