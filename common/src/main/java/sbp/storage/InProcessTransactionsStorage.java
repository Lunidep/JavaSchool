package sbp.storage;

import sbp.dto.TransactionDto;

import java.util.Map;

public interface InProcessTransactionsStorage {
    Map<String, TransactionDto> getTransactionsSendInProgress();
    boolean isTransactionsSendInProgressEmpty();
    void putTransactionSendInProgress(TransactionDto transaction);
    void removeTransactionSendInProgress(String id);
}
