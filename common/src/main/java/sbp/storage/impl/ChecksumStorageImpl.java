package sbp.storage.impl;

import lombok.RequiredArgsConstructor;
import sbp.dto.TransactionDto;
import sbp.storage.ChecksumStorage;
import sbp.storage.Storage;

import java.util.Map;
import java.util.stream.Collectors;

import static sbp.utils.ChecksumHelper.calculateChecksum;

@RequiredArgsConstructor
public class ChecksumStorageImpl implements ChecksumStorage {
    private final Storage storage;

    @Override
    public Map<Long, String> getSentChecksumMap() {
        return storage.getSentChecksumMap();
    }

    @Override
    public String getSentCheckSum(long intervalKey) {
        return storage.getSentChecksumMap().get(intervalKey);
    }

    @Override
    public void updateCheckSum(long intervalKey) {
        storage.getSentChecksumMap().put(intervalKey, calculateChecksum(
                storage.getSentTransactions().get(intervalKey)
                        .stream()
                        .map(TransactionDto::getTransactionId)
                        .collect(Collectors.toList())));
    }
}