package io.vectorsync.worker.service.embedding;

public class EmbeddingException extends Exception {
    public EmbeddingException(String message) {
        super(message);
    }

    public EmbeddingException(String message, Throwable cause) {
        super(message, cause);
    }
}
