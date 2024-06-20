package eu.cymo.avro_composer.adapter.kafka.stream;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.springframework.stereotype.Component;

import eu.cymo.avro_composer.adapter.kafka.TopicsConfig;
import eu.cymo.avro_composer.adapter.kafka.avro.AvroSchemaService;
import eu.cymo.avro_composer.adapter.kafka.avro.CompositionSchemaService;
import io.micrometer.tracing.Tracer;

@Component
public class Processors {
    private final TopicsConfig topics;
    private final CompositionConfig composition;
    private final AvroSchemaService avroSchemaService;
    private final CompositionSchemaService compositionSchemaService;
    private final Tracer tracer;
    
    public Processors(
            TopicsConfig topics,
            CompositionConfig composition,
            AvroSchemaService avroSchemaService,
            CompositionSchemaService compositionSchemaService,
            Tracer tracer) {
        this.topics = topics;
        this.composition = composition;
        this.avroSchemaService = avroSchemaService;
        this.compositionSchemaService = compositionSchemaService;
        this.tracer = tracer;
    }
    
    public ProcessorSupplier<Bytes, GenericRecord, Bytes, GenericRecord> composition() {
        return () -> wrapInTracer(
                "composition-process", 
                new CompositionProcessor(
                    avroSchemaService,
                    compositionSchemaService,
                    composition,
                    topics));
    }
    
    private <KIn, VIn, KOut, VOut> Processor<KIn, VIn, KOut, VOut> wrapInTracer(String tag, Processor<KIn, VIn, KOut, VOut> target) {
        return new TracingProcessor<>(tracer, tag, target);
    }
    
}
