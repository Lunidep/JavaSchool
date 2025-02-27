package sbp.storage.ackconsumerstorage.impl;

import lombok.RequiredArgsConstructor;
import sbp.dto.TransactionDto;
import sbp.storage.ackconsumerstorage.SentTransactionsStorage;
import sbp.storage.ackconsumerstorage.Storage;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

@RequiredArgsConstructor
public class SentTransactionsStorageImpl implements SentTransactionsStorage {
    private final Storage storage;

    @Override
    public List<TransactionDto> getSentTransactions(long intervalKey) {
        return storage.getSentTransactions().get(intervalKey);
    }

    @Override
    public boolean isSentTransactionsEmpty() {
        return storage.getSentTransactions().isEmpty();
    }

    @Override
    public Set<Long> getSentTransactionIntervalKeys() {
        return storage.getSentTransactions().keySet();
    }

    @Override
    public void putSentTransaction(long intervalKey, TransactionDto transaction) {
        storage.getSentTransactions().computeIfAbsent(intervalKey, k -> new ArrayList<>()).add(transaction);
    }

    @Override
    public void cleanupInterval(Long intervalKey) {
        storage.getSentTransactions().remove(intervalKey);
        storage.getSentChecksumMap().remove(intervalKey);
    }
}
