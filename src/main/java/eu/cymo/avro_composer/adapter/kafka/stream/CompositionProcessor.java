package eu.cymo.avro_composer.adapter.kafka.stream;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;

import eu.cymo.avro_composer.adapter.kafka.TopicsConfig;
import eu.cymo.avro_composer.adapter.kafka.avro.CompositionSchema;
import eu.cymo.avro_composer.adapter.kafka.avro.FieldBuilder;
import eu.cymo.avro_composer.adapter.kafka.avro.GenericRecordAdapter;
import eu.cymo.avro_composer.adapter.kafka.avro.RegistrationException;
import eu.cymo.avro_composer.adapter.kafka.avro.SchemaBuilder;
import eu.cymo.avro_composer.adapter.kafka.avro.SchemaVersionService;
import eu.cymo.avro_composer.adapter.kafka.avro.SubjectAvroSchemaService;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.serializers.subject.TopicRecordNameStrategy;

public class CompositionProcessor implements Processor<Bytes, GenericRecord, Bytes, GenericRecord> {
    private final SubjectAvroSchemaService schemaService;
    private final SchemaVersionService versionService;
    private final TopicsConfig topics;
    private final CompositionConfig composition;
    
    private ProcessorContext<Bytes, GenericRecord> context;
    
    public CompositionProcessor(
            SubjectAvroSchemaService schemaService,
            SchemaVersionService versionService,
            TopicsConfig topics,
            CompositionConfig composition) {
        this.schemaService = schemaService;
        this.versionService = versionService;
        this.topics = topics;
        this.composition = composition;
    }
    
    @Override
    public void init(ProcessorContext<Bytes, GenericRecord> context) {
        this.context = context;
    }
    
    @Override
    public void process(Record<Bytes, GenericRecord> record) {
        try {
            var value = record.value();

            var newValue = schemaService.getLatestSchema()
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
        // if the existing schema has a matching schema in the event union,
        // it's still possible we have a mismatch of versions
        if(schema.eventUnionHasSchemaWithNameAndNamespace(value.getSchema())) {
            // the schema version is the same, just continue
            if(schema.eventUnionContains(value.getSchema())) {
                return composedGenericRecord(schema, value);
            }
            // we have a version mismatch, we have to decide which version is newer
            else {
                
                var newerSchema = getNewerVersionSafe(
                        schema.getSchemaWithNameAndNameSpace(value.getSchema()),
                        value.getSchema());
                // we already are working with the newer version, no need to update
                // the composition schema
                if(schema.eventUnionContains(newerSchema)) {
                    var newValue = GenericRecordAdapter.adaptToNewSchema(value, newerSchema);
                    return composedGenericRecord(schema, newValue);
                }
                // the new schema is a newer version, update the composition
                else {
                    var updatedSchema = updateCompositionSchema(schema, value);
                    registerSafe(updatedSchema, value);
                    return composedGenericRecord(updatedSchema, value);
                }
            }
        }
        // schema not present in event union type
        // add it
        else {
            var updatedSchema = updateCompositionSchema(schema, value);
            registerSafe(updatedSchema, value);
            return composedGenericRecord(updatedSchema, value);
        }
    }
    
    private Schema getNewerVersionSafe(Schema left, Schema right) {
        try {
            return versionService.getNewerSchema(
                    inputTopicValueSubject(left),
                    left,
                    right);
        }
        catch(Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    private String inputTopicValueSubject(Schema schema) {
        return new TopicRecordNameStrategy().subjectName(topics.getInput(), false, new AvroSchema(schema));
    }
    
    private GenericRecord processNewSchema(GenericRecord value) {
        var newSchema = createCompositionSchema(value);
        registerSafe(newSchema, value);
        return composedGenericRecord(newSchema, value);
    }
    
    private void registerSafe(CompositionSchema schema, GenericRecord value) {
        try {
            schemaService.register(schema);
        } catch (RegistrationException e) {
            throw new RuntimeException("Error occured while adding schema '%s' to the composed schema".formatted(value.getSchema().getName()), e);
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
                .name(composition.getName())
                .documentation(composition.getDocumentation())
                .namespace(composition.getNamespace());
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
                .namespace(composition.getNamespace())
                .build();
    }
}
