package com.example.flink.common; 

import org.apache.flink.util.StringUtils;

public class Converter {
    public static double toDoubleSafety(String str) {
        try {
            return StringUtils.isNullOrWhitespaceOnly(str) ? 0.0f : Double.parseDouble(str);
        } catch (java.lang.NumberFormatException ex) {
            System.out.println("fail to parse " + str + " to double");
            return 0.0f;
        }
    }
}