package sbp.storage;

public interface ChecksumStorage {
    String getSentCheckSum(long intervalKey);
    void updateCheckSum(long intervalKey);
}