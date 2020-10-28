package com.example.flink.common;

public class MathUtils {
    public static final double ESP_DOUBLE = 0.001;
    public static float leftPercent(double left, double right) {
        double acc = left + right;
        return (acc < ESP_DOUBLE) ? 0 : (float)(left / acc);
    }
}
