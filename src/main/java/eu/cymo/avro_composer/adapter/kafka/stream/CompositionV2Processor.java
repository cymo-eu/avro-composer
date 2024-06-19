package eu.cymo.avro_composer.adapter.kafka.stream;

import java.util.Optional;

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
import eu.cymo.avro_composer.adapter.kafka.avro.OtherAvroSchemaService;
import eu.cymo.avro_composer.adapter.kafka.avro.SchemaBuilder;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.serializers.subject.TopicRecordNameStrategy;

public class CompositionV2Processor implements Processor<Bytes, GenericRecord, Bytes, GenericRecord> {
    private final OtherAvroSchemaService service;
    private final CompositionConfig composition;
    private final TopicsConfig topics;
    
    private ProcessorContext<Bytes, GenericRecord> context;
    
    public CompositionV2Processor(
            OtherAvroSchemaService service,
            CompositionConfig composition,
            TopicsConfig topics) {
        this.service = service;
        this.composition = composition;
        this.topics = topics;
    }
    
    @Override
    public void init(ProcessorContext<Bytes, GenericRecord> context) {
        this.context = context;
    }

    @Override
    public void process(Record<Bytes, GenericRecord> record) {
        var value = record.value();
        
        var composed = Optional.ofNullable(service.getLatestSchema(composition.getSubject()))
            .map(CompositionSchema::new)
            .map(s -> processExistingSchema(s, value))
            .orElseGet(() -> processNewSchema(value));
        
//        System.out.println();
//        System.out.println("Forwarding " + composed.getSchema().getName() + ": " + composed);
        context.forward(
                new Record<>(
                        record.key(),
                        composed,
                        record.timestamp(),
                        record.headers()));
    }
    
    private GenericRecord processExistingSchema(CompositionSchema composition, GenericRecord value) {
        if(composition.eventUnionHasSchemaWithNameAndNamespace(value.getSchema())) {
            var eventSchema = composition.getSchemaWithNameAndNameSpace(value.getSchema());
            
            var eventSchemaVersion = version(eventSchema);
            var valueVersion = version(value.getSchema());
            
            if(valueVersion > eventSchemaVersion) {
                // update schema
                var schema = value.getSchema();
                service.register(this.composition.getSubject(), updateCompositionSchema(composition, schema).schema());
                return composedGenericRecord(composition, value);
            }
            else {
                // value has out of date schema, update value to be compliant
                var newValue = GenericRecordAdapter.adaptToNewSchema(value, eventSchema);
                return composedGenericRecord(composition, newValue);
            }
        }
        else {
            var latestSchema = service.getLatestSchema(inputTopicValueSubject(value.getSchema()));
            
            service.register(this.composition.getSubject(), updateCompositionSchema(composition, latestSchema).schema());

            var newValue = GenericRecordAdapter.adaptToNewSchema(value, latestSchema);
            return composedGenericRecord(composition, newValue);
        }
    }
    
    private GenericRecord processNewSchema(GenericRecord value) {
        var latest = service.getLatestSchema(inputTopicValueSubject(value.getSchema()));
        
        var composition = createCompositionSchema(latest);
        var newValue = GenericRecordAdapter.adaptToNewSchema(value, latest);
        return composedGenericRecord(composition, newValue);
    }
    
    private int version(Schema schema) {
        return service.getVersion(inputTopicValueSubject(schema), schema);
    }
    
    private String inputTopicValueSubject(Schema schema) {
        return new TopicRecordNameStrategy().subjectName(topics.getInput(), false, new AvroSchema(schema));
    }
    
    private CompositionSchema updateCompositionSchema(CompositionSchema composition, Schema schema) {
        return new CompositionSchema(
                compositionSchema()
                    .field(updateEventField(composition, schema))
                    .build());
    }
    
    private GenericRecord composedGenericRecord(CompositionSchema schema, GenericRecord value) {
        var newValue = new GenericData.Record(schema.schema());
        newValue.put(CompositionSchema.EVENT_FIELD, value);
        return newValue;
    }
    
    private SchemaBuilder compositionSchema() {
        return SchemaBuilder.newBuilder()
                .name(composition.getName())
                .documentation(composition.getDocumentation())
                .namespace(composition.getNamespace());
    }
    
    private Field updateEventField(CompositionSchema composition, Schema schema) {
        return eventField()
                .types(composition.eventFieldUnionTypes())
                .type(schema)
                .build();
    }
    
    private Field generateEventField(Schema schema) {
        return eventField()
                .type(unknown())
                .type(schema)
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
    
    private CompositionSchema createCompositionSchema(Schema schema) {
        return new CompositionSchema(
                compositionSchema()
                    .field(generateEventField(schema))
                    .build());
    }

}
