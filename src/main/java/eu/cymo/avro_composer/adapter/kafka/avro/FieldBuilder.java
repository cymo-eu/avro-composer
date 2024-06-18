package eu.cymo.avro_composer.adapter.kafka.avro;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;

public class FieldBuilder {
    private String name;
    private String documentation;
    private Object defaultValue;
    
    private Map<String, Schema> types = new LinkedHashMap<>();
    
    private FieldBuilder() {}
    
    public static FieldBuilder newBuilder() {
        return new FieldBuilder();
    }

    public FieldBuilder name(String name) {
        this.name = name;
        return this;
    }

    public FieldBuilder documentation(String documentation) {
        this.documentation = documentation;
        return this;
    }

    public FieldBuilder defaultValue(Object defaultValue) {
        this.defaultValue = defaultValue;
        return this;
    }
    
    public FieldBuilder type(Schema type) {
        this.types.put(type.getFullName(), type);
        return this;
    }

    public FieldBuilder types(List<Schema> types) {
        types.forEach(this::type);
        return this;
    }
    
    public FieldBuilder defaultNull() {
        return defaultValue(Field.NULL_DEFAULT_VALUE);
    }
    
    public Field build() {
        assert name != null : new IllegalArgumentException("'name' cannot be null");
        assert types().size() > 0 : new IllegalArgumentException("'types' cannot be empty");
        
        return new Field(
                name,
                generateSchema(),
                documentation,
                defaultValue);
    }
    
    private List<Schema> types() {
        return types.values()
                .stream()
                .toList();
    }
    
    private Schema generateSchema() {
        if(types().size() == 1) {
            return types().get(0);
        }
        return Schema.createUnion(types());
    }
}
