package com.example.flink.model;


import lombok.*;

@NoArgsConstructor
@AllArgsConstructor
@Builder
@Getter
@Setter
public class PriceIntradayBar {
    private String symbol;
    private Long timeSec;

    private double open;
    private double close;
    private double high;
    private double low;
    private long volume;
    private double cumulativeVol;
    private double changeVal;
}
