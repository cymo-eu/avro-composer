package eu.cymo.avro_composer.adapter.kafka.stream;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.springframework.stereotype.Component;

import eu.cymo.avro_composer.adapter.kafka.avro.SubjectAvroSchemaService;

@Component
public class Processors {
    private final SubjectAvroSchemaService subjectAvroSchemaService;
    private final CompositionConfig compositionConfig;
    
    public Processors(
            SubjectAvroSchemaService subjectAvroSchemaService,
            CompositionConfig compositionConfig) {
        this.subjectAvroSchemaService = subjectAvroSchemaService;
        this.compositionConfig = compositionConfig;
    }
    
    public ProcessorSupplier<Bytes, GenericRecord, Bytes, GenericRecord> composition() {
        return () -> new CompositionProcessor(
                subjectAvroSchemaService,
                compositionConfig);
    }
    
}
