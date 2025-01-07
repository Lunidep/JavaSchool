package sbp.storage.ackconsumerstorage;

public interface ChecksumStorage {
    String getSentCheckSum(long intervalKey);
    void updateCheckSum(long intervalKey);
}