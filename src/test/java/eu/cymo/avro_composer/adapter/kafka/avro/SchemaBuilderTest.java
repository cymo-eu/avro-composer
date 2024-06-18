package eu.cymo.avro_composer.adapter.kafka.avro;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Arrays;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.junit.jupiter.api.Test;

class SchemaBuilderTest {

    @Test
    void throwException_whenNameMissing() {
        // given
        var builder = SchemaBuilder.newBuilder()
                .namespace("namespace");
        
        // then
        assertThatThrownBy(builder::build)
            .isInstanceOf(AssertionError.class);
    }

    @Test
    void throwException_whenNamespaceMissing() {
        // given
        var builder = SchemaBuilder.newBuilder()
                .name("name");
        
        // then
        assertThatThrownBy(builder::build)
            .isInstanceOf(AssertionError.class);
    }
    
    @Test
    void setsFields() {
        // when
        var schema = SchemaBuilder.newBuilder()
                .name("name")
                .documentation("documentation")
                .namespace("namespace")
                .field(intField())
                .build();
        
        // then
        assertThat(schema.getName()).isEqualTo("name");
        assertThat(schema.getDoc()).isEqualTo("documentation");
        assertThat(schema.getNamespace()).isEqualTo("namespace");
        assertThat(schema.getField("int")).isEqualTo(intField());
    }
    
    @Test
    void setMultipleFields() {
        // when
        var schema = SchemaBuilder.newBuilder()
                .name("name")
                .namespace("namespace")
                .fields(Arrays.asList(intField(), stringField()))
                .build();
        
        // then
        assertThat(schema.getField("int")).isEqualTo(intField());
        assertThat(schema.getField("string")).isEqualTo(stringField());
    }
    
    @Test
    void setMultipleFields_appendsToFields() {
        // when
        var schema = SchemaBuilder.newBuilder()
                .name("name")
                .namespace("namespace")
                .field(intField())
                .field(stringField())
                .build();
        
        // then
        assertThat(schema.getField("int")).isEqualTo(intField());
        assertThat(schema.getField("string")).isEqualTo(stringField());
    }
    
    @Test
    void overwriteField_ifSameName() {
        // when
        var schema = SchemaBuilder.newBuilder()
                .name("name")
                .namespace("namespace")
                .field(stringField())
                .field(doubleFieldWithNameString())
                .build();
        
        // then
        assertThat(schema.getField("string")).isEqualTo(doubleFieldWithNameString());
    }

    private Field intField() {
        return FieldBuilder.newBuilder()
                .name("int")
                .type(Schema.create(Schema.Type.INT))
                .build();
    }

    private Field stringField() {
        return FieldBuilder.newBuilder()
                .name("string")
                .type(Schema.create(Schema.Type.STRING))
                .build();
    }

    private Field doubleFieldWithNameString() {
        return FieldBuilder.newBuilder()
                .name("string")
                .type(Schema.create(Schema.Type.DOUBLE))
                .build();
    }
}
