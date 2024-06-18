package eu.cymo.avro_composer.adapter.kafka.stream;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;

import eu.cymo.avro_composer.adapter.kafka.avro.CompositionSchema;
import eu.cymo.avro_composer.adapter.kafka.avro.FieldBuilder;
import eu.cymo.avro_composer.adapter.kafka.avro.RegistrationException;
import eu.cymo.avro_composer.adapter.kafka.avro.SchemaBuilder;
import eu.cymo.avro_composer.adapter.kafka.avro.SubjectAvroSchemaService;

public class CompositionProcessor implements Processor<Bytes, GenericRecord, Bytes, GenericRecord> {
    private final SubjectAvroSchemaService service;
    private final CompositionConfig config;
    
    private ProcessorContext<Bytes, GenericRecord> context;
    
    public CompositionProcessor(
            SubjectAvroSchemaService service,
            CompositionConfig config) {
        this.service = service;
        this.config = config;
    }
    
    @Override
    public void init(ProcessorContext<Bytes, GenericRecord> context) {
        this.context = context;
    }
    
    @Override
    public void process(Record<Bytes, GenericRecord> record) {
        try {
            var value = record.value();

            var newValue = service.getLatestSchema()
                .map(schema -> processExistingSchema(schema, value))
                .orElseGet(() -> processNewSchema(value));
            
            context.forward(
                    new Record<>(
                            record.key(),
                            newValue,
                            record.timestamp(),
                            record.headers()));
        }
        catch(Exception e) {
            throw new RecoverableProcessingException(e.getMessage(), e);
        }
    }
    
    private GenericRecord processExistingSchema(CompositionSchema schema, GenericRecord value) {
        if(schema.eventUnionContains(value.getSchema())) {
            return composedGenericRecord(schema, value);
        }
        else {
            var updatedSchema = updateCompositionSchema(schema, value);
            registerSafe(updatedSchema);
            return composedGenericRecord(updatedSchema, value);
        }
    }
    
    private GenericRecord processNewSchema(GenericRecord value) {
        var newSchema = createCompositionSchema(value);
        registerSafe(newSchema);
        return composedGenericRecord(newSchema, value);
    }
    
    private void registerSafe(CompositionSchema schema) {
        try {
            service.register(schema);
        } catch (RegistrationException e) {
            throw new RuntimeException(e);
        }
    }
    
    private GenericRecord composedGenericRecord(CompositionSchema schema, GenericRecord value) {
        var newValue = new GenericData.Record(schema.schema());
        newValue.put(CompositionSchema.EVENT_FIELD, value);
        return newValue;
    }
    
    private CompositionSchema createCompositionSchema(GenericRecord value) {
        return new CompositionSchema(
                compositionSchema()
                    .field(generateEventField(value))
                    .build());
    }
    
    private CompositionSchema updateCompositionSchema(CompositionSchema schema, GenericRecord value) {
        return new CompositionSchema(
                compositionSchema()
                    .field(updateEventField(schema, value))
                    .build());
    }
    
    private SchemaBuilder compositionSchema() {
        return SchemaBuilder.newBuilder()
                .name(config.getName())
                .documentation(config.getDocumentation())
                .namespace(config.getNamespace());
    }
    
    private Field updateEventField(CompositionSchema schema, GenericRecord value) {
        return eventField()
                .types(schema.eventFieldUnionTypes())
                .type(value.getSchema())
                .build();
    }
    
    private Field generateEventField(GenericRecord value) {
        return eventField()
                .type(unknown())
                .type(value.getSchema())
                .build();
    }
    
    private FieldBuilder eventField() {
        return FieldBuilder.newBuilder()
                .name(CompositionSchema.EVENT_FIELD)
                .documentation(null);
    }
    
    private Schema unknown() {
        return SchemaBuilder.newBuilder()
                .name("Unknown")
                .namespace(config.getNamespace())
                .build();
    }
}
