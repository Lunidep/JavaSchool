package sbp.utils;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;

public final class ChecksumCalculator {

    public static String calculateChecksum(List<String> transactionIds) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            String concatenatedIds = String.join("", transactionIds);

            md.update(concatenatedIds.getBytes());
            byte[] digest = md.digest();

            return new BigInteger(1, digest).toString(16);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("Ошибка вычисления контрольной суммы", e);
        }
    }
}
