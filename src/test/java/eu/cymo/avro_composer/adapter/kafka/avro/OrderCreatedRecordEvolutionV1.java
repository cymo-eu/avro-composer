package eu.cymo.avro_composer.adapter.kafka.avro;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

public class OrderCreatedRecordEvolutionV1 extends GenericRecordBuilder {
    private String orderId;
    
    private OrderCreatedRecordEvolutionV1() {} 
    
    public OrderCreatedRecordEvolutionV1 orderId(String orderId) {
        this.orderId = orderId;
        return this;
    }
    
    public GenericRecord build() {
        var schema = ClasspathSchemas.orderCreatedRecordEvolutionV1();
        
        var record = new GenericData.Record(schema);
        
        if(orderId != null) {
            var order = new GenericData.Record(getChildSchema(schema, "order"));
            
            record.put("order", order);
            order.put("orderId", orderId);
        }
        
        return record;
    }
    
    public static OrderCreatedRecordEvolutionV1 newBuilder() {
        return new OrderCreatedRecordEvolutionV1();
    }
}
