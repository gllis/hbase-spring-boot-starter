package com.gllis.hbase.api;

import org.apache.hadoop.hbase.client.Table;

/**
 * 表操作回调
 *
 * @author glli
 * @date 2023/8/10
 */
public interface TableCallBack<T> {

    /**
     * 操作表
     *
     * @param table active Hbase table
     * @return a result object, or null if none
     * @throws Throwable thrown by the Hbase API
     */
    T doInTable(Table table) throws Throwable;
}
