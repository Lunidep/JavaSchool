package sbp.storage;

import java.util.Map;

public interface ChecksumStorage {
    Map<Long, String> getSentChecksumMap();
    String getSentCheckSum(long intervalKey);
    void updateCheckSum(long intervalKey);
}