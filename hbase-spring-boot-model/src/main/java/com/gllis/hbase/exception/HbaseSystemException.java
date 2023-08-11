package com.gllis.hbase.exception;

/**
 * Hbase Access Exception
 *
 * @author glli
 * @date 2023/8/10
 */
public class HbaseSystemException extends RuntimeException {

    public HbaseSystemException(Exception cause) {
        super(cause.getMessage(), cause);
    }

    public HbaseSystemException(Throwable throwable) {
        super(throwable.getMessage(), throwable);
    }
}
