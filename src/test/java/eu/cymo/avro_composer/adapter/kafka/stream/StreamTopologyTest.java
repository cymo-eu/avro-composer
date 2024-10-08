package eu.cymo.avro_composer.adapter.kafka.stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.context.annotation.FilterType.REGEX;

import java.io.IOException;
import java.util.Optional;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.ComponentScan;

import eu.cymo.avro_composer.adapter.kafka.TopicsConfig;
import eu.cymo.avro_composer.adapter.kafka.avro.AvroSerdeFactory;
import eu.cymo.avro_composer.adapter.kafka.avro.OrderCreatedArrayEvolutionV1;
import eu.cymo.avro_composer.adapter.kafka.avro.OrderCreatedArrayEvolutionV2;
import eu.cymo.avro_composer.adapter.kafka.avro.OrderCreatedRecordEvolutionV1;
import eu.cymo.avro_composer.adapter.kafka.avro.OrderCreatedRecordEvolutionV2;
import eu.cymo.avro_composer.adapter.kafka.stream.topology.TopologyTest;
import eu.cymo.kafkaSerializationEvolution.event.OrderConfirmed;
import eu.cymo.kafkaSerializationEvolution.event.OrderDelivered;
import eu.cymo.kafkaSerializationEvolution.event.OrderShipped;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

@TopologyTest(
        includeFilters = { 
                @ComponentScan.Filter(type = REGEX, pattern = { "eu.cymo.avro_composer.adapter.kafka.*" })})
public class StreamTopologyTest {
    @Autowired
    private TopicsConfig topics;
    @Autowired
    private CompositionConfig composition;
    
    @Autowired
    private AvroSerdeFactory avroSerdes;
    @Autowired
    private SchemaRegistryClient schemaRegistry;
    
    @Autowired
    private TopologyTestDriver driver;

    private TestInputTopic<String, GenericRecord> input;
    private TestOutputTopic<String, GenericRecord> output;
    
    @BeforeEach
    void setup() {
        input = driver.createInputTopic(
                topics.getInput(),
                Serdes.String().serializer(),
                avroSerdes.genericAvroValueSerde().serializer());
        output = driver.createOutputTopic(
                topics.getOutput(),
                Serdes.String().deserializer(),
                avroSerdes.genericAvroValueSerde().deserializer());
    }

    @AfterEach
    void clearTopics() {
        output.readValuesToList();
    }
    
    /**
     * This tests covers the initial state of stream, when no schema has been
     * defined yet for the output topic. This test verifies a new schema is
     * created and that the output value has the expected structure.
     */
    @Test
    void whenNoOutputSchema_createSchemaFromInput() {
        // given
        var orderConfirmed = orderConfirmed();
        
        // when
        input.pipeInput(orderConfirmed);
        
        // then
        var result = output.readValue();
        var eventSchema = getOutputEventFieldSchema();
        
        assertThat(result.get("event").toString()).isEqualTo(orderConfirmed.toString());
        assertThat(eventSchema.getTypes()).contains(orderConfirmed.getSchema());
    }
    
    /**
     * The output schema should have an event field that is a union. The initial
     * record of this union should be an unknown schema without any fields. The
     * other schemas of the output schema should contain schemas of previously
     * posted messages on the input topic.
     */
    @Test
    void outputSchemaHasUnknownSchemaAsPosition0ForEvent() {
        // when
        input.pipeInput(orderConfirmed());
        
        // then
        var eventSchema = getOutputEventFieldSchema();
        
        assertThat(eventSchema.getTypes())
            .first()
            .satisfies(
                    s -> assertThat(s.getName()).isEqualTo("Unknown"),
                    s -> assertThat(s.getFields()).isEmpty());
        
    }
    
    /**
     * This test verifies that when a schema for the output already exists
     * and on the input a record is posted with a schema unknown to the output
     * that it will be added to the 'event' field of the output schema.
     */
    @Test
    void whenOutputSchemaExists_updateOutputSchema_onNewSchemaOnInput() {
        // given
        var orderConfirmed = orderConfirmed();
        var orderShipped = orderShipped();
        
        // when
        input.pipeInput(orderConfirmed);
        input.pipeInput(orderShipped);
        
        // then
        var result = output.readValuesToList();
        var eventSchema = getOutputEventFieldSchema();
        
        assertThat(result)
            .last()
            .satisfies(r -> assertThat(r.get("event").toString()).isEqualTo(orderShipped.toString()));
        assertThat(eventSchema.getTypes()).contains(orderShipped.getSchema());
    }
    
    /**
     * This test verifies that when no new schemas are posted on the input,
     * that the output schema will not change.
     */
    @Test
    void whenNoNewSchemaPostedOnInput_dontUpdateOutputSchema() {
        // when
        input.pipeInput(orderConfirmed());
        var eventSchema1 = getOutputEventFieldSchema();
        input.pipeInput(orderConfirmed());
        var eventSchema2 = getOutputEventFieldSchema();
        
        // then
        assertThat(eventSchema1).isEqualTo(eventSchema2);
    }
    
    /**
     * This test verifies that the event field has a union of all input
     * record schemas.
     */
    @Test
    void outputSchemaHasEventField_withUnionOfAllPreviouslyPostInputSchemas() {
        // given
        var orderConfirmed = orderConfirmed();
        var orderShipped = orderShipped();
        var orderDelivered = orderDelivered();
        
        // when
        input.pipeInput(orderConfirmed);
        input.pipeInput(orderShipped);
        input.pipeInput(orderDelivered);
        
        // then
        var eventSchema = getOutputEventFieldSchema();
        
        assertThat(eventSchema.getTypes()).contains(
                orderConfirmed.getSchema(),
                orderShipped.getSchema(),
                orderDelivered.getSchema());
    }
    
    /**
     * When a newer version of a schema is available, update the output
     * schema with the newer version.
     */
    @Test
    void supportsRecordEvolution() {
        // given
        var createdV1 = OrderCreatedRecordEvolutionV1.newBuilder()
                .orderId("order-id")
                .build();
        var createdV2 = OrderCreatedRecordEvolutionV2.newBuilder()
                .orderId("order-id")
                .customerId("customer-id")
                .build();
        
        // when
        input.pipeInput(createdV1);
        input.pipeInput(createdV2);
        
        // then
        var result = output.readValuesToList();
        var eventSchema = getOutputEventFieldSchema();
        
        assertThat(result)
            .satisfiesExactly(
                    item1 -> assertThat(item1.get("event").toString()).isEqualTo(createdV1.toString()),
                    item2 -> assertThat(item2.get("event").toString()).isEqualTo(createdV2.toString()));
        assertThat(eventSchema.getTypes()).contains(createdV2.getSchema());
    }
    
    /**
     * When a newer version of a schema is available, update the output
     * schema with the newer version.
     */
    @Test
    void supportsArrayEvolution() {
        // given
        var createdV1 = OrderCreatedArrayEvolutionV1.newBuilder()
                .order(new OrderCreatedArrayEvolutionV1.Order("order-id"))
                .build();
        var createdV2 = OrderCreatedArrayEvolutionV2.newBuilder()
                .order(new OrderCreatedArrayEvolutionV2.Order("order-id", "customerId"))
                .build();
        
        // when
        input.pipeInput(createdV1);
        input.pipeInput(createdV2);
        
        // then
        var result = output.readValuesToList();
        var eventSchema = getOutputEventFieldSchema();
        
        assertThat(result)
            .satisfiesExactly(
                    item1 -> assertThat(item1.get("event").toString()).isEqualTo(createdV1.toString()),
                    item2 -> assertThat(item2.get("event").toString()).isEqualTo(createdV2.toString()));
        assertThat(eventSchema.getTypes()).contains(createdV2.getSchema());
    }
    
    private Schema getOutputEventFieldSchema() {
        return getOutputSchema().getField("event").schema();
    }
    
    private Schema getOutputSchema() {
        try {
            return Optional.ofNullable(schemaRegistry.getLatestSchemaMetadata(composition.getSubject()))
                    .map(SchemaMetadata::getSchema)
                    .map(new Schema.Parser()::parse)
                    .orElseThrow(() -> new RuntimeException("No metadata found for subject '%s'".formatted(composition.getSubject())));
        } catch (IOException | RestClientException e) {
            throw new RuntimeException("Failed to retrieve the latest schema metadata for subject '%s'".formatted(composition.getSubject()), e);
        }
    }
    
    private OrderConfirmed orderConfirmed() {
        return OrderConfirmed.newBuilder()
                .setOrderId("order-id")
                .build();
    }
    
    private OrderDelivered orderDelivered() {
        return OrderDelivered.newBuilder()
                .setOrderId("order-id")
                .setDeliveryPartner("delivery-partner")
                .build();
    }
    
    private OrderShipped orderShipped() {
        return OrderShipped.newBuilder()
                .setOrderId("order-id")
                .setDeliveryPartner("delivery-partner")
                .build();
    }
}
