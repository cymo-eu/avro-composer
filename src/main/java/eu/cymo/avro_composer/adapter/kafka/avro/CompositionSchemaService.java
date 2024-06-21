package eu.cymo.avro_composer.adapter.kafka.avro;

import java.util.Optional;

import org.springframework.stereotype.Component;

import eu.cymo.avro_composer.adapter.kafka.stream.CompositionConfig;

@Component
public class CompositionSchemaService {
    private final AvroSchemaService schemaService;
    private final CompositionConfig composition;
    
    private CompositionSchema cachedSchema;
    
    public CompositionSchemaService(
            AvroSchemaService schemaService,
            CompositionConfig composition) {
        this.schemaService = schemaService;
        this.composition = composition;
    }
    
    public void register(CompositionSchema compositionSchema) throws RegistrationException {
        try {
            schemaService.register(composition.getSubject(), compositionSchema.schema());
            setCachedSchema(compositionSchema);
        }
        catch (RegistrationException e) {
            setCachedSchema(null);
            throw e;
        }
    }
    
    public void invalidateCachedValue() {
        setCachedSchema(null);
    }
    
    public CompositionSchema getLatestSchema() {
        if(getCachedSchema() == null) {
            Optional.ofNullable(schemaService.getLatestSchema(composition.getSubject()))
                .map(CompositionSchema::new)
                .ifPresent(this::setCachedSchema);
        }
        return getCachedSchema();
    }
    
    private void setCachedSchema(CompositionSchema cachedSchema) {
        this.cachedSchema = cachedSchema;
    }
    
    private CompositionSchema getCachedSchema() {
        return cachedSchema;
    }
    
}
