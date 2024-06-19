package eu.cymo.avro_composer.adapter.kafka.stream;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.springframework.stereotype.Component;

import eu.cymo.avro_composer.adapter.kafka.TopicsConfig;
import eu.cymo.avro_composer.adapter.kafka.avro.SchemaVersionService;
import eu.cymo.avro_composer.adapter.kafka.avro.SubjectAvroSchemaService;

@Component
public class Processors {
    private final SubjectAvroSchemaService schemaService;
    private final SchemaVersionService versionService;
    private final TopicsConfig topics;
    private final CompositionConfig composition;
    
    public Processors(
            SubjectAvroSchemaService schemaService,
            SchemaVersionService versionService,
            TopicsConfig topics,
            CompositionConfig composition) {
        this.schemaService = schemaService;
        this.versionService = versionService;
        this.topics = topics;
        this.composition = composition;
    }
    
    public ProcessorSupplier<Bytes, GenericRecord, Bytes, GenericRecord> composition() {
        return () -> new CompositionProcessor(
                schemaService,
                versionService,
                topics,
                composition);
    }
    
}
