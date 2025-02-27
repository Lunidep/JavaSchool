package sbp.storage.ackproducerstorage;


import sbp.dto.TransactionDto;

import java.util.List;
import java.util.Set;

public interface TransactionStorage {
    void addTransactionByProducer(String producerId, TransactionDto transaction);
    List<TransactionDto> getTransactionsByProducer(String producerId);
    Set<String> getProducerIds();
    void removeTransactions(String producerId, List<String> transactionIds);
    boolean isEmpty();
}
