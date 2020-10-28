package com.example.flink.customer;

import com.example.flink.model.PriceIntradayBar;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.Properties;


public class PriceIntradaySink extends DatabaseSink1<PriceIntradayBar> {
    public PriceIntradaySink(Properties rdsConfig) {
        super(rdsConfig);
    }

    public String getQuery() {
        return "insert into " + this.rdsConfig.get("table.name") + " ( " +
                "t_ticker, i_seq_time, f_open_price, f_close_price, f_high_price, f_low_price, f_open_price_change, i_volume, i_acc_volume ) " +
                "values (?,?,?,?,?,?,?,?,?)";
    }

    @Override
    public void pInvoke(PriceIntradayBar value, SinkFunction.Context context) throws Exception {
        preparedStatement.setString(1, value.getSymbol());
        preparedStatement.setLong(2, value.getTimeSec());
        preparedStatement.setFloat(3, (float) value.getOpen());
        preparedStatement.setFloat(4, (float) value.getClose());
        preparedStatement.setFloat(5, (float) value.getHigh());
        preparedStatement.setFloat(6, (float) value.getLow());
        preparedStatement.setFloat(7, (float) value.getChangeVal());
        preparedStatement.setLong(8, value.getVolume());
        preparedStatement.setLong(9, (long) value.getCumulativeVol());
        preparedStatement.executeUpdate();
    }
}
