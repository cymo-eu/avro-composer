package eu.cymo.avro_composer.adapter.kafka.stream;

public class RecoverableProcessingException extends RuntimeException {
    private static final long serialVersionUID = 6143713199439584697L;

    public RecoverableProcessingException(String message, Throwable throwable) {
        super(message, throwable);
    }

    public RecoverableProcessingException(Throwable throwable) {
        super(throwable);
    }
    
}
