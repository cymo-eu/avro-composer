package eu.cymo.avro_composer.adapter.kafka.avro;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.avro.JsonProperties;
import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;

class FieldBuilderTest {

    @Test
    void throwException_whenNameMissing() {
        // given
        var builder = FieldBuilder.newBuilder()
                .type(Schema.create(Schema.Type.STRING));
        
        // then
        assertThatThrownBy(builder::build)
            .isInstanceOf(AssertionError.class);
    }
    
    @Test
    void throwsException_whenTypesEmpty() {
        // given
        var builder = FieldBuilder.newBuilder()
                .name("Name");
        
        // then
        assertThatThrownBy(builder::build)
            .isInstanceOf(AssertionError.class);
        
    }
    
    @Test
    void createsField() {
        // when
        var field = FieldBuilder.newBuilder()
                .name("Name")
                .documentation("documentation")
                .defaultValue("default-value")
                .type(Schema.create(Schema.Type.STRING))
                .build();
        
        // then
        assertThat(field.name()).isEqualTo("Name");
        assertThat(field.doc()).isEqualTo("documentation");
        assertThat(field.defaultVal()).isEqualTo("default-value");
        assertThat(field.schema()).isEqualTo(Schema.create(Schema.Type.STRING));
    }
    
    @Test
    void createsAUnion_whenMultipleTypesDefined() {
        // when
        var field = FieldBuilder.newBuilder()
                .name("Name")
                .type(Schema.create(Schema.Type.STRING))
                .type(Schema.create(Schema.Type.INT))
                .build();

        // then
        assertThat(field.schema().getTypes()).contains(
                Schema.create(Schema.Type.STRING),
                Schema.create(Schema.Type.INT));
    }
    
    @Test
    void setsNULL_DEFAULT_VALUE_whenDefaultNull() {
        // when
        var field = FieldBuilder.newBuilder()
                .name("Name")
                .type(Schema.create(Schema.Type.NULL))
                .type(Schema.create(Schema.Type.STRING))
                .defaultNull()
                .build();

        // then
        assertThat(field.defaultVal()).isEqualTo(JsonProperties.NULL_VALUE);
    }

}
