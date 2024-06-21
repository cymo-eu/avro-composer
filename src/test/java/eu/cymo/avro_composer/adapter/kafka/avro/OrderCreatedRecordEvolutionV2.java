package eu.cymo.avro_composer.adapter.kafka.avro;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

public class OrderCreatedRecordEvolutionV2 extends GenericRecordBuilder {
    private String orderId;
    private String customerId;
    
    private OrderCreatedRecordEvolutionV2() {} 
    
    public OrderCreatedRecordEvolutionV2 orderId(String orderId) {
        this.orderId = orderId;
        return this;
    }
    
    public OrderCreatedRecordEvolutionV2 customerId(String customerId) {
        this.customerId = customerId;
        return this;
    }
    
    public GenericRecord build() {
        var schema = ClasspathSchemas.orderCreatedRecordEvolutionV2();
        
        var record = new GenericData.Record(schema);
        
        if(orderId != null) {
            var order = new GenericData.Record(getChildSchema(schema, "order"));
            
            record.put("order", order);
            order.put("orderId", orderId);
            order.put("customerId", customerId);
        }
        
        return record;
    }
    
    public static OrderCreatedRecordEvolutionV2 newBuilder() {
        return new OrderCreatedRecordEvolutionV2();
    }
}
