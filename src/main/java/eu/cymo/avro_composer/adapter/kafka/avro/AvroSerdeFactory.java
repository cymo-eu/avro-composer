package eu.cymo.avro_composer.adapter.kafka.avro;

import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;

public interface AvroSerdeFactory {

    default GenericAvroSerde genericAvroValueSerde() {
        return genericAvroSerde(false);
    }
    
    public GenericAvroSerde genericAvroSerde(boolean isKey);

}
