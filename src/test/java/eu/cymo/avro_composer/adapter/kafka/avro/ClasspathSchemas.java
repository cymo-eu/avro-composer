package eu.cymo.avro_composer.adapter.kafka.avro;

import java.io.IOException;

import org.apache.avro.Schema;

public class ClasspathSchemas {
    private static final String AVRO = "avro";

    private static final String RECORD_SCHEMA_EVOLUTION = AVRO + "/record-schema-evolution";
    private static final String RECORD_EVOLUATION_ORDER_CREATED_V1 = RECORD_SCHEMA_EVOLUTION + "/order-created-v1.avsc";
    private static final String RECORD_EVOLUATION_ORDER_CREATED_V2 = RECORD_SCHEMA_EVOLUTION + "/order-created-v2.avsc";

    private static final String ARRAY_SCHEMA_EVOLUTION = AVRO + "/array-schema-evolution";
    private static final String ARRAY_EVOLUATION_ORDER_CREATED_V1 = ARRAY_SCHEMA_EVOLUTION + "/order-created-v1.avsc";
    private static final String ARRAY_EVOLUATION_ORDER_CREATED_V2 = ARRAY_SCHEMA_EVOLUTION + "/order-created-v2.avsc";
    
    private ClasspathSchemas() {}
    
    public static Schema orderCreatedRecordEvolutionV1() {
        return load(RECORD_EVOLUATION_ORDER_CREATED_V1);
    }
    
    public static Schema orderCreatedRecordEvolutionV2() {
        return load(RECORD_EVOLUATION_ORDER_CREATED_V2);
    }
    
    public static Schema orderCreatedArrayEvolutionV1() {
        return load(ARRAY_EVOLUATION_ORDER_CREATED_V1);
    }
    
    public static Schema orderCreatedArrayEvolutionV2() {
        return load(ARRAY_EVOLUATION_ORDER_CREATED_V2);
    }
    
    private static Schema load(String path) {
        try(var in = ClasspathSchemas.class.getClassLoader().getResourceAsStream(path)) {
            return new Schema.Parser().parse(in);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
