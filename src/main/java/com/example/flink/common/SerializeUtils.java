package com.example.flink.common;

import com.google.protobuf.GeneratedMessageV3;

public class SerializeUtils {
    public static byte[] serializeProto2Q(String code, int sec, String symbol, GeneratedMessageV3 proto) {
        String s = code
                + "S" + sec
                + Constants.MSG_CODE_SEPARATOR + symbol
                + Constants.MSG_CODE_SEPARATOR + Base64Utils.encodeBytes(proto.toByteArray(), "");
        return s.getBytes();
    }

    public static byte[] serializeProto2Q(String code, String symbol, GeneratedMessageV3 proto) {
        String s = code
                + Constants.MSG_CODE_SEPARATOR + symbol
                + Constants.MSG_CODE_SEPARATOR + Base64Utils.encodeBytes(proto.toByteArray(), "");
        return s.getBytes();
    }
}
