package eu.cymo.avro_composer.adapter.kafka.stream;

import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.cymo.avro_composer.adapter.kafka.TopicsConfig;
import eu.cymo.avro_composer.adapter.kafka.avro.AvroSchemaService;
import eu.cymo.avro_composer.adapter.kafka.avro.CompositionSchema;
import eu.cymo.avro_composer.adapter.kafka.avro.CompositionSchemaService;
import eu.cymo.avro_composer.adapter.kafka.avro.Fields;
import eu.cymo.avro_composer.adapter.kafka.avro.GenericRecordAdapter;
import eu.cymo.avro_composer.adapter.kafka.avro.GenericRecords;
import eu.cymo.avro_composer.adapter.kafka.avro.Schemas;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.serializers.subject.TopicRecordNameStrategy;

public class CompositionProcessor implements Processor<Bytes, GenericRecord, Bytes, GenericRecord> {
    private final AvroSchemaService avroSchemaService;
    private final CompositionSchemaService compositionSchemaService;
    private final CompositionConfig composition;
    private final TopicsConfig topics;
    
    private ProcessorContext<Bytes, GenericRecord> context;
    
    private final Logger log = LoggerFactory.getLogger(CompositionProcessor.class);
    
    public CompositionProcessor(
            AvroSchemaService service,
            CompositionSchemaService compositionSchemaService,
            CompositionConfig composition,
            TopicsConfig topics) {
        this.avroSchemaService = service;
        this.compositionSchemaService = compositionSchemaService;
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
        
        var composed = Optional.ofNullable(compositionSchemaService.getLatestSchema())
            .map(s -> processExistingSchema(s, value))
            .orElseGet(() -> processNewSchema(value));
        
        context.forward(
                new Record<>(
                        record.key(),
                        composed,
                        record.timestamp(),
                        record.headers()));
    }
    
    private GenericRecord processExistingSchema(CompositionSchema compositionSchema, GenericRecord value) {
        if(compositionSchema.eventUnionHasSchemaWithNameAndNamespace(value.getSchema())) {
            var eventSchema = compositionSchema.getSchemaWithNameAndNameSpace(value.getSchema());
            
            var eventSchemaVersion = version(eventSchema);
            var valueVersion = version(value.getSchema());
            
            if(valueVersion > eventSchemaVersion) {
                // we are working with an outdated composition schema
                // invalidate the cache and rebuild
                compositionSchemaService.invalidateCachedValue();
                
                var updatedCompositionSchema = updateCompositionSchemaToLatest();
                compositionSchemaService.register(updatedCompositionSchema);
                
                var newValue = adaptToNewSchema(value, updatedCompositionSchema.getSchemaWithNameAndNameSpace(value.getSchema()));
                return GenericRecords.composed(updatedCompositionSchema, newValue);
            }
            else {
                // value has out of date schema, update value to be compliant
                var newValue = adaptToNewSchema(value, eventSchema);
                return GenericRecords.composed(compositionSchema, newValue);
            }
        }
        else {
            var latestSchema = avroSchemaService.getLatestSchema(inputTopicValueSubject(value.getSchema()));
            var updatedCompositionSchema = addSchemaToCompositionSchema(compositionSchema, latestSchema);
            
            compositionSchemaService.register(updatedCompositionSchema);

            var newValue = adaptToNewSchema(value, latestSchema);
            return GenericRecords.composed(updatedCompositionSchema, newValue);
        }
    }
    
    private GenericRecord processNewSchema(GenericRecord value) {
        var latest = avroSchemaService.getLatestSchema(inputTopicValueSubject(value.getSchema()));
        var compositionSchema = newCompositionSchema(latest);
        
        compositionSchemaService.register(compositionSchema);
        
        var newValue = adaptToNewSchema(value, latest);
        return GenericRecords.composed(compositionSchema, newValue);
    }
    
    private GenericRecord adaptToNewSchema(GenericRecord value, Schema latest) {
        try {
            return GenericRecordAdapter.adaptToNewSchema(value, latest);
        }
        catch(Exception e) {
            log.error("Adapter failure:");
            log.error("Record: {}", value);
            log.error("Record schema: {}", value.getSchema());
            log.error("Latest schema: {}", latest);
            throw new RuntimeException(e);
        }
    }
    
    /**
     * For every union schema in event field (excluding 'Schemas.SCHEMA_UNKNOWN')
     * bring the schema to the latest version. This is to avoid mismatches in
     * nested record schemas.
     */
    private CompositionSchema updateCompositionSchemaToLatest() {
        var subjects = compositionSchemaService.getLatestSchema()
                .eventFieldUnionTypes()
                .stream()
                .filter(Predicate.not(s -> s.getName().equals(Schemas.SCHEMA_UNKNOWN)))
                .map(this::inputTopicValueSubject)
                .toList();
        
        var schemas = subjects.stream()
                .map(avroSchemaService::getLatestSchema)
                .toList();
        
        return new CompositionSchema(
                Schemas.composition(
                        composition,
                        Fields.event(composition, schemas)));
    }
    
    private int version(Schema schema) {
        return avroSchemaService.getVersion(inputTopicValueSubject(schema), schema);
    }
    
    private String inputTopicValueSubject(Schema schema) {
        return new TopicRecordNameStrategy().subjectName(topics.getInput(), false, new AvroSchema(schema));
    }
    
    private CompositionSchema addSchemaToCompositionSchema(CompositionSchema currentSchema, Schema schema) {
        return new CompositionSchema(
                Schemas.composition(
                        composition,
                        Fields.event(
                                composition,
                                Stream.concat(
                                        currentSchema.eventFieldUnionTypes().stream(), 
                                        Stream.of(schema)).toList())));
    }
    
    private CompositionSchema newCompositionSchema(Schema schema) {
        return new CompositionSchema(
                Schemas.composition(
                        composition,
                        Fields.event(composition, schema)));
    }

}
