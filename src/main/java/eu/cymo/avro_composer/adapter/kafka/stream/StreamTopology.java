package eu.cymo.avro_composer.adapter.kafka.stream;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import eu.cymo.avro_composer.adapter.kafka.TopicsConfig;
import eu.cymo.avro_composer.adapter.kafka.avro.AvroSerdeFactory;

@Component
public class StreamTopology {
    private final TopicsConfig topics;
    private final AvroSerdeFactory avroSerdes;
    private final Processors processors;
    
    public StreamTopology(
            TopicsConfig topics,
            AvroSerdeFactory avroserdes,
            Processors processors) {
        this.topics = topics;
        this.avroSerdes = avroserdes;
        this.processors = processors;
    }
    
    @Autowired
    public void configure(StreamsBuilder builder) {
        builder.stream(topics.getInput(),
                       Consumed.with(Serdes.Bytes(),
                                     avroSerdes.genericAvroValueSerde()))
               .process(processors.composition())
               .to(topics.getOutput(),
                   Produced.with(Serdes.Bytes(),
                                 avroSerdes.genericAvroValueSerde()));
    }
}
