package sbp.storage;

import java.util.Map;

public interface RetryCountStorage {
    Map<String, Integer> getRetryCountMap();
    int getRetryCount(String transactionId);
    void putRetryCount(String transactionId, int retryCount);
    void removeRetryCount(String transactionId);
}