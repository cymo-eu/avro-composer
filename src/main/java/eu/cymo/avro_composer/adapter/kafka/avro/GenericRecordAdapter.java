package eu.cymo.avro_composer.adapter.kafka.avro;

import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

public class GenericRecordAdapter {

    /**
     * If the composition event schema has a newer version than the active record,
     * we need to update the record to compliant with the newer version
     */
    public static GenericRecord adaptToNewSchema(GenericRecord record, Schema newSchema) {
        var newRecord = new GenericData.Record(newSchema);
        
        var oldValues = getValues(record);
        
        for(var field : newSchema.getFields()) {
            var value = oldValues.get(field.name());
            if(value != null) {
                if(value instanceof GenericRecord oldValue) {
                    newRecord.put(field.name(), adaptToNewSchema(oldValue, findRecordSchema(oldValue, field)));
                }
                else if(value instanceof GenericData.Array oldArray) {
                    var array = new GenericData.Array<>(oldArray.size(), findArrayWhenUnion(field));
                    for(var item : oldArray) {
                        array.add(processArrayItem(item, findArrayElementType(field)));
                    }
                    newRecord.put(field.name(), array);
                }
                else {
                    newRecord.put(field.name(), value);
                }
            }
            
            else {
                if(field.defaultVal() != null) {
                    newRecord.put(field.name(), field.defaultVal());
                }
            }
        }
        
        return newRecord;
    }
    
    private static Schema findRecordSchema(GenericRecord record, Field field) {
        var oldSchema = record.getSchema();
        if(field.schema().getType() == Schema.Type.UNION) {
            return field.schema()
                    .getTypes()
                    .stream()
                    .filter(s -> Objects.equals(s.getFullName(), oldSchema.getFullName()))
                    .findFirst()
                    .orElseThrow(() -> new IllegalArgumentException("Failed to find schema '%s' in union field '%s'".formatted(oldSchema.getName(), field)));
        }
        return field.schema();
    }
    
    private static Schema findArrayWhenUnion(Field field) {
        if(field.schema().getType() == Schema.Type.UNION) {
            return field.schema()
                    .getTypes()
                    .stream()
                    .filter(s -> s.getType() == Type.ARRAY)
                    .findFirst()
                    .orElseThrow(() -> new IllegalArgumentException("Failed to find array schema in union field '%s'".formatted(field)));
        }
        return field.schema();
    }
    
    private static Schema findArrayElementType(Field field) {
        return findArrayWhenUnion(field).getElementType();
    }
    
    private static Object processArrayItem(Object item, Schema schema) {
        if(schema.getType() == Schema.Type.RECORD) {
            return adaptToNewSchema((GenericRecord) item, schema);
        }
        else {
            return item;
        }
    }
    
    private static Map<String, Object> getValues(GenericRecord record) {
        return record.getSchema()
                .getFields()
                .stream()
                .filter(f -> Objects.nonNull(record.get(f.name())))
                .collect(Collectors.toMap(
                        Field::name,
                        f -> record.get(f.name())));
    }
}
