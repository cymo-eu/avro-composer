package eu.cymo.avro_composer.adapter.kafka.avro;

public class RetrieveMetadataException extends Exception {
    private static final long serialVersionUID = 2116010435530089282L;

    public RetrieveMetadataException(String message, Throwable throwable) {
        super(message, throwable);
    }
}
