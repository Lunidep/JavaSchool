package sbp.storage;

public interface RetryCountStorage {
    int getRetryCount(String transactionId);
    void putRetryCount(String transactionId, int retryCount);
}