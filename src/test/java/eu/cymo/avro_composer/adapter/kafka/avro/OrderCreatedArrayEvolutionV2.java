package eu.cymo.avro_composer.adapter.kafka.avro;

import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

public class OrderCreatedArrayEvolutionV2 extends GenericRecordBuilder {
    private List<Order> orders = new ArrayList<>();
    
    private OrderCreatedArrayEvolutionV2() {} 
    
    public OrderCreatedArrayEvolutionV2 order(Order order) {
        this.orders.add(order);
        return this;
    }
    
    public GenericRecord build() {
        var schema = ClasspathSchemas.orderCreatedArrayEvolutionV2();
        
        var record = new GenericData.Record(schema);
        
        if(!orders.isEmpty()) {
            var ordersSchema = getChildSchema(schema, "order");
            var orderSchema = ordersSchema.getElementType();
            
            var ordersArray = new GenericData.Array<>(orders.size(), ordersSchema);
            ordersArray.addAll(orders.stream()
                    .map(o -> order(orderSchema, o))
                    .toList());
            record.put("order", ordersArray);
        }
        
        return record;
    }
    
    private GenericRecord order(Schema schema, Order order) {
        var orderRecord = new GenericData.Record(schema);
        orderRecord.put("orderId", order.orderId());
        orderRecord.put("customerId", order.customerId());
        return orderRecord;
    }
    
    public static OrderCreatedArrayEvolutionV2 newBuilder() {
        return new OrderCreatedArrayEvolutionV2();
    }
    
    public record Order(String orderId, String customerId) { }
}
