package com.example.flink.common;


import java.util.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Base64Utils {
    private static Logger logger = LoggerFactory.getLogger(Base64Utils.class);
	/**
	 * Convert Base64 Encoding string reversed to original byte[]
	 * 
	 * @param uncompressed
	 * @return original bytes
	 */
	public static byte[] decodeBytes(String uncompressed) {
		try {
			return Base64.getDecoder().decode(uncompressed);
		} catch (Exception e) {
            logger.error(e.getMessage());
		}
		return null;
	}

	/**
	 * Convert byte[] to Base64 Encoding
	 * 
	 * @param strBytes
	 *            input bytes
	 * @return Base64 String to transmit
	 */
	public static String encodeBytes(byte[] strBytes, String defaultIfFail) {
		try {
			return Base64.getEncoder().encodeToString(strBytes);
		} catch (Exception e) {
            logger.error(e.getMessage());
            
			return defaultIfFail;
        }
    }
}
