package com.example.flink.customer;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Properties;

abstract class DatabaseSink1<IN> extends RichSinkFunction<IN> {
    protected Connection connection;
    protected PreparedStatement preparedStatement;
    protected final Properties rdsConfig;
    protected static Logger logger = LoggerFactory.getLogger(DatabaseSink1.class);

    private String dbUsername;
    private String dbPassword;
    private String dbDrivername;
    private String dbUrl;
    private String mSql;

    protected DatabaseSink1(Properties rdsConfig) {
        this.rdsConfig = rdsConfig;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        dbUsername = this.rdsConfig.getProperty("username");
        dbPassword = this.rdsConfig.getProperty("password");
        dbDrivername = this.rdsConfig.getProperty("drivername");
        dbUrl = this.rdsConfig.getProperty("url");

        Class.forName(dbDrivername);
        createCnn();

        super.open(parameters);
    }

    private void createCnn() throws Exception {
        connection = DriverManager.getConnection(dbUrl, dbUsername, dbPassword);
        mSql = this.getQuery();
        preparedStatement = connection.prepareStatement(mSql);
    }

    private void retryInvoke(IN t, Context context) throws Exception {
        pInvoke(t, context);
    }

    private void verifyCnn() throws Exception {
        if (connection.isClosed() || !connection.isValid(1)) {
            createCnn();
        }
    }

    protected abstract String getQuery();
    protected abstract void pInvoke(IN t, Context context) throws Exception;

    @Override
    public void invoke(IN t, Context context) throws Exception {
        try {
            pInvoke(t, context);
        } catch (Exception ex) {
            logger.error("invoke exception {} {}", ex.getMessage(), ex);
            try {
                verifyCnn();
                retryInvoke(t, context);
            } catch (Exception ex1) {
                logger.error("reinvoke still exception {} {}", ex.getMessage(), ex);
                throw ex1;
            }
        }
    }

    @Override
    public void close() throws Exception {
        if(preparedStatement != null){
            preparedStatement.close();
        }
        if(connection != null){
            connection.close();
        }
        super.close();
    }

}