package sbp.storage.ackconsumerstorage.impl;

import lombok.RequiredArgsConstructor;
import sbp.dto.TransactionDto;
import sbp.storage.ackconsumerstorage.ChecksumStorage;
import sbp.storage.ackconsumerstorage.Storage;

import java.util.stream.Collectors;

import static sbp.utils.ChecksumCalculator.calculateChecksum;

@RequiredArgsConstructor
public class ChecksumStorageImpl implements ChecksumStorage {
    private final Storage storage;

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