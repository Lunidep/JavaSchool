package sbp.storage.ackproducerstorage.impl;

import sbp.dto.TransactionDto;
import sbp.storage.ackproducerstorage.TransactionStorage;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class TransactionStorageImpl implements TransactionStorage {
    private final Map<String, List<TransactionDto>> transactions = new ConcurrentHashMap<>();


    @Override
    public void addTransactionByProducer(String producerId, TransactionDto transaction) {
        transactions.computeIfAbsent(producerId, k -> new ArrayList<>()).add(transaction);
    }

    @Override
    public List<TransactionDto> getTransactionsByProducer(String producerId) {
        return transactions.get(producerId);
    }

    @Override
    public Set<String> getProducerIds() {
        return transactions.keySet();
    }

    @Override
    public void removeTransactions(String producerId, List<String> transactionIds) {
        if (transactionIds.isEmpty()) {
            return;
        }

        List<TransactionDto> producerTransactions = transactions.get(producerId);
        if (producerTransactions == null || producerTransactions.isEmpty()) {
            return;
        }

        Set<String> transactionIdSet = new HashSet<>(transactionIds);

        synchronized (producerTransactions) {
            boolean removed = producerTransactions.removeIf(transaction ->
                    transactionIdSet.contains(transaction.getTransactionId())
            );

            if (removed && producerTransactions.isEmpty()) {
                transactions.remove(producerId);
            }
        }
    }

    @Override
    public boolean isEmpty() {
        return transactions.isEmpty();
    }
}
