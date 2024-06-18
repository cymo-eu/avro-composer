package eu.cymo.avro_composer.adapter.kafka.avro;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;

public class SchemaBuilder {
    private String name;
    private String documentation;
    private String namespace;
    
    private Map<String, Field> fields = new LinkedHashMap<>();
    
    private SchemaBuilder() {}
    
    public static SchemaBuilder newBuilder() {
        return new SchemaBuilder();
    }

    public SchemaBuilder name(String name) {
        this.name = name;
        return this;
    }

    public SchemaBuilder documentation(String documentation) {
        this.documentation = documentation;
        return this;
    }

    public SchemaBuilder namespace(String namespace) {
        this.namespace = namespace;
        return this;
    }

    public SchemaBuilder field(Field field) {
        this.fields.put(field.name(), field);
        return this;
    }
    
    public SchemaBuilder fields(List<Field> fields) {
        fields.forEach(this::field);
        return this;
    }
    
    public Schema build() {
        assert name != null : new IllegalArgumentException("'name' cannot be null");
        assert namespace != null : new IllegalArgumentException("'namespace' cannot be null");
        
        return Schema.createRecord(
                name,
                documentation,
                namespace,
                false,
                fields.values()
                    .stream()
                    .map(field -> new Field(field, field.schema()))
                    .toList());
    }
}
