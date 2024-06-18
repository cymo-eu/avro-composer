package eu.cymo.avro_composer.adapter.kafka.stream;

import static org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD;
import static org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_CLIENT;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KafkaStreams.State;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.kafka.StreamsBuilderFactoryBeanCustomizer;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.kafka.config.StreamsBuilderFactoryBeanConfigurer;

@Configuration
public class StreamsBeanConfig {

    @Bean
    StreamsBuilderFactoryBeanConfigurer streamsBuilderFactoryConfigurer() {
        return new StreamsBuilderFactoryBeanConfigurer() {
            
            private static final Logger logger = LoggerFactory.getLogger(StreamsBuilderFactoryBeanConfigurer.class);
            
            @Override
            public void configure(StreamsBuilderFactoryBean factoryBean) {
                factoryBean.setStreamsUncaughtExceptionHandler(ex -> {
                    if(ex instanceof RecoverableProcessingException) {
                        logger.error("Recoverable error occurred, replacing stream thread.", ex);
                        return REPLACE_THREAD;
                    }
                    
                    logger.error("Unknown error occurred, shutting down stream application.", ex);
                    return SHUTDOWN_CLIENT;
                });
            }
        };
    }

    @Bean
    StreamsBuilderFactoryBeanCustomizer kafkaAppKillerRegistration(ApplicationContext ctx) {
        return (streamsBuilderFactoryBean) -> streamsBuilderFactoryBean.setStateListener(new KafkaStreams.StateListener() {
            
            @Override
            public void onChange(State newState, State oldState) {
                if(newState == State.ERROR) {
                    SpringApplication.exit(ctx);
                    System.exit(1);
                }
            }
        });
    }
    
}
