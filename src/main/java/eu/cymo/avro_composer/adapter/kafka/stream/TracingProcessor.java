package eu.cymo.avro_composer.adapter.kafka.stream;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;

import io.micrometer.tracing.Tracer;

public class TracingProcessor<KIn, VIn, KOut, VOut> implements Processor<KIn, VIn, KOut, VOut> {
    private final Tracer tracer;
    private final String tag;
    private final Processor<KIn, VIn, KOut, VOut> target;
    
    public TracingProcessor(
            Tracer tracer,
            String tag,
            Processor<KIn, VIn, KOut, VOut> target) {
        this.tracer = tracer;
        this.tag = tag;
        this.target = target;
    }
    
    @Override
    public void init(ProcessorContext<KOut, VOut> context) {
        target.init(context);
    }
    
    @Override
    public void process(Record<KIn, VIn> record) {
        var span = tracer.startScopedSpan(tag);
        try {
            target.process(record);
        }
        finally {
            span.end();
        }
    }
    
    @Override
    public void close() {
        target.close();
    }

}
