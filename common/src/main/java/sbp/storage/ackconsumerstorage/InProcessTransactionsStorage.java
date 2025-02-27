package sbp.storage.ackconsumerstorage;

import sbp.dto.TransactionDto;

public interface InProcessTransactionsStorage {
    boolean isTransactionsSendInProgressEmpty();
    void putTransactionSendInProgress(TransactionDto transaction);
    void removeTransactionSendInProgress(String id);
}
