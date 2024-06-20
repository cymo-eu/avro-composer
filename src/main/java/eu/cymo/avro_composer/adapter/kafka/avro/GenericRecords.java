package eu.cymo.avro_composer.adapter.kafka.avro;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

public class GenericRecords {

    private GenericRecords() {}
    
    public static GenericRecord composed(CompositionSchema compositionSchema, GenericRecord event) {
        var composed = new GenericData.Record(compositionSchema.schema());
        composed.put(CompositionSchema.EVENT_FIELD, event);
        return composed;
    }
    
}
