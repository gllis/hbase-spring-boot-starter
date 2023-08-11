package com.gllis.hbase.exception;

/**
 * Hbase connection Exception
 *
 * @author glli
 * @date 2023/8/10
 */
public class HbaseConnectionException extends RuntimeException {

    public HbaseConnectionException(Exception cause) {
        super(cause.getMessage(), cause);
    }

    public HbaseConnectionException(Throwable throwable) {
        super(throwable.getMessage(), throwable);
    }
}
