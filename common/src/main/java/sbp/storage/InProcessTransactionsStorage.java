package sbp.storage;

import sbp.dto.TransactionDto;

import java.util.Map;

public interface InProcessTransactionsStorage {
    boolean isTransactionsSendInProgressEmpty();
    void putTransactionSendInProgress(TransactionDto transaction);
    void removeTransactionSendInProgress(String id);
}
