package eu.cymo.avro_composer.adapter.kafka.avro;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;

import eu.cymo.avro_composer.adapter.kafka.stream.CompositionConfig;

public class Schemas {
    public static final String SCHEMA_UNKNOWN = "Unknown";

    private Schemas() {}
    
    public static Schema unknown(CompositionConfig composition) {
        return SchemaBuilder.newBuilder()
                .name(SCHEMA_UNKNOWN)
                .namespace(composition.getNamespace())
                .build();
    }
    
    public static SchemaBuilder compositionBuilder(CompositionConfig composition) {
        return SchemaBuilder.newBuilder()
                .name(composition.getName())
                .documentation(composition.getDocumentation())
                .namespace(composition.getNamespace());
    }
    
    public static Schema composition(CompositionConfig composition, Field field) {
        return SchemaBuilder.newBuilder()
                .name(composition.getName())
                .documentation(composition.getDocumentation())
                .namespace(composition.getNamespace())
                .field(field)
                .build();
    }
    
}
