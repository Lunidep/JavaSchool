package sbp.storage;

import lombok.Data;
import sbp.dto.TransactionDto;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Data
public class Storage {
    public final Map<Long, List<TransactionDto>> sentTransactions = new ConcurrentHashMap<>();
    public final Map<String, Integer> retryCountMap = new ConcurrentHashMap<>();
    public final Map<Long, String> sentChecksumMap = new ConcurrentHashMap<>();
    public final Map<String, TransactionDto> transactionsSendInProgress = new ConcurrentHashMap<>();
}
