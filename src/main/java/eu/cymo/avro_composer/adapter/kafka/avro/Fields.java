package eu.cymo.avro_composer.adapter.kafka.avro;

import java.util.Collections;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;

import eu.cymo.avro_composer.adapter.kafka.stream.CompositionConfig;

public class Fields {
    
    private Fields() {}
    
    public static FieldBuilder eventFieldBuilder() {
        return FieldBuilder.newBuilder()
                .name(CompositionSchema.EVENT_FIELD)
                .documentation(null);
    }
    
    public static Field event(CompositionConfig composition, Schema schema) {
        return Fields.event(composition, Collections.singletonList(schema));
    }
    
    public static Field event(CompositionConfig composition, List<Schema> schemas) {
        return Fields.eventFieldBuilder()
                .type(Schemas.unknown(composition))
                .types(schemas)
                .build();
    }
    
}
