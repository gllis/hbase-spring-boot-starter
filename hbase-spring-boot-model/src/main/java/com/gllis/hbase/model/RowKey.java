package com.gllis.hbase.model;

/**
 * RowKey interface
 *
 * @author glli
 * @date 2023/8/10
 */
public interface RowKey {

    /**
     * Convert row key to bytes.
     * 
     * @return row key's bytes.
     * */
    public byte[] toBytes();
}
