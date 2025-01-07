package sbp.utils;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

public final class IntervalCalculator {

    public static long getIntervalKey(LocalDateTime time, Duration interval) {
        long timeMilliseconds = time.atZone(ZoneOffset.UTC)
                .toInstant()
                .toEpochMilli();
        long intervalMilliseconds = interval.toMillis();
        return timeMilliseconds / intervalMilliseconds;
    }
}
