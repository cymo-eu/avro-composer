package eu.cymo.avro_composer.adapter.kafka.avro;

public class RegistrationException extends Exception {
    private static final long serialVersionUID = -4720842845644328410L;

    public RegistrationException(String message, Throwable throwable) {
        super(message, throwable);
    }
    
}
