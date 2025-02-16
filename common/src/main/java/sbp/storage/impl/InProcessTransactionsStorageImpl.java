package sbp.storage.impl;

import lombok.RequiredArgsConstructor;
import sbp.dto.TransactionDto;
import sbp.storage.InProcessTransactionsStorage;
import sbp.storage.Storage;

@RequiredArgsConstructor
public class InProcessTransactionsStorageImpl implements InProcessTransactionsStorage {
    private final Storage storage;

    @Override
    public boolean isTransactionsSendInProgressEmpty() {
        return storage.getTransactionsSendInProgress().isEmpty();
    }

    @Override
    public void putTransactionSendInProgress(TransactionDto transaction) {
        storage.getTransactionsSendInProgress().put(transaction.getTransactionId(), transaction);
    }

    @Override
    public void removeTransactionSendInProgress(String id) {
        storage.getTransactionsSendInProgress().remove(id);
    }

}
