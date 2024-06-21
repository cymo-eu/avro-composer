package eu.cymo.avro_composer.adapter.kafka.avro;

import org.apache.avro.Schema;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Component;

import io.micrometer.tracing.Tracer;

@Component
@Primary
public class TracingAvroSchemaService implements AvroSchemaService {
    private final AvroSchemaService target;
    private final Tracer tracer;
    
    public TracingAvroSchemaService(
            AvroSchemaService target,
            Tracer tracer) {
        this.target = target;
        this.tracer = tracer;
    }

    @Override
    public void register(String subject, Schema schema) throws RegistrationException {
        var span = tracer.startScopedSpan("avro-schema-service-register");
        try {
            target.register(subject, schema);
        }
        finally {
            span.end();
        }
    }

    @Override
    public Schema getLatestSchema(String subject) {
        var span = tracer.startScopedSpan("avro-schema-service-get-latest-schema");
        try {
            return target.getLatestSchema(subject);
        }
        finally {
            span.end();
        }
    }

    @Override
    public int getVersion(String subject, Schema schema) throws RetrieveVersionException {
        var span = tracer.startScopedSpan("avro-schema-service-get-version");
        try {
            return target.getVersion(subject, schema);
        }
        finally {
            span.end();
        }
    }
    
    
}
