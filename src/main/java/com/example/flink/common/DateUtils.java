package com.example.flink.common;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.*;

public class DateUtils {
    public static long timestampAtHourAndMinute(String tz, int hour, int min) {
        Calendar now = Calendar.getInstance(TimeZone.getTimeZone(tz));
        now.set(Calendar.HOUR_OF_DAY, hour);
        now.set(Calendar.MINUTE, min);
        now.set(Calendar.SECOND, 0);

        return now.getTimeInMillis()/1000;
    }

    public static long timestampAtHourAndMinute(String tz, Map<String, Integer> hm) {
        return timestampAtHourAndMinute(tz, hm.get("hour"), hm.get("min"));
    }

    public static long currentTimestamp() {
        return Calendar.getInstance().getTimeInMillis()/1000;
    }

    public static Map<String, Integer> parseHourAndMinute(String hm) {
        Map<String, Integer> map = new HashMap<>();
        String[] arrHourMin = hm.split(":");
        map.put("hour", Integer.parseInt(arrHourMin[0]));
        map.put("min", Integer.parseInt(arrHourMin[1]));
        return map;
    }

    public static long fromTimeWindow(long timewindow) {
        Timestamp ts = new Timestamp(timewindow);
        Date date = new Date(ts.getTime());
        Instant instant = date.toInstant();

        return instant.getEpochSecond();
    }
}
