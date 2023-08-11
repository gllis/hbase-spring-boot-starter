package com.gllis.hbase.api;


import com.gllis.hbase.annotaion.HbaseColumn;
import com.gllis.hbase.annotaion.HbaseColumnIgnore;
import com.gllis.hbase.exception.HbaseConnectionException;
import com.gllis.hbase.exception.HbaseSystemException;
import com.gllis.hbase.model.Column;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.exceptions.HBaseException;
import org.apache.log4j.Logger;
import org.springframework.util.Assert;
import org.springframework.util.StopWatch;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Hbase 操作类
 *
 * @author glli
 * @date 2023/8/10
 */
public class HbaseOperations {

    public static final Logger log = Logger.getLogger(HbaseOperations.class);
    private static Map<String, Map<String, Column>> familyCacheMap = new ConcurrentHashMap<>();
    private static Map<String, Map<String, Boolean>> columnIgnoreCacheMap = new ConcurrentHashMap<>();

    private Configuration configuration;
    private volatile Connection connection;

    public void setConfiguration(Configuration configuration) {
        this.configuration = configuration;
    }

    public Connection getConnection() {
        if (null == this.connection) {
            synchronized (this) {
                if (null == this.connection) {
                    try {
                        ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(10, 30, 60L, TimeUnit.SECONDS, new SynchronousQueue<>());
                        threadPoolExecutor.prestartCoreThread();
                        this.connection = ConnectionFactory.createConnection(configuration, threadPoolExecutor);
                    } catch (IOException e) {
                        log.error("Hbase 连接资源池创建失败");
                        throw new HbaseConnectionException(e);
                    }
                }
            }
        }
        return this.connection;
    }

    /**
     * 获取列注解
     *
     * @param clazzName  类名
     * @param fields     字段
     * @return
     */
    public Map<String, Column> getColumnMap(String clazzName, Field[] fields) {
        Map<String, Column> columnMap = familyCacheMap.get(clazzName);
        if (columnMap != null) {
            return columnMap;
        }
        columnMap = new HashMap<>();
        for (Field field : fields) {
            field.setAccessible(true);
            int mod = field.getModifiers();
            // 静态方式或常量不映射
            if (Modifier.isStatic(mod) || Modifier.isFinal(mod)) {
                continue;
            }
            HbaseColumn column = field.getAnnotation(HbaseColumn.class);
            if (column == null) {
                continue;
            }
            columnMap.put(field.getName(), new Column(column.family(), column.value()));
        }
        familyCacheMap.put(clazzName, columnMap);

        return columnMap;
    }

    /**
     * 获取忽略的字段
     *
     * @param clazzName
     * @param fields
     * @return
     */
    public Map<String, Boolean> getColumnIgnoreMap(String clazzName, Field[] fields) {
        Map<String, Boolean> columnIgnoreMap = columnIgnoreCacheMap.get(clazzName);
        if (columnIgnoreMap != null) {
            return columnIgnoreMap;
        }
        columnIgnoreMap = new HashMap<>();
        for (Field field : fields) {
            field.setAccessible(true);
            int mod = field.getModifiers();
            if (Modifier.isStatic(mod) || Modifier.isFinal(mod)) {
                continue;
            }
            HbaseColumnIgnore column = field.getAnnotation(HbaseColumnIgnore.class);
            if (column == null) {
                columnIgnoreMap.put(field.getName(), false);
            } else {
                columnIgnoreMap.put(field.getName(), true);
            }
        }
        return columnIgnoreMap;
    }

    /**
     * Hbase 执行表操作
     *
     * @param tableName  表名
     * @param action     表回调
     * @return 操作结果
     */
    public <T> T execute(String tableName, TableCallBack<T> action) {
        Assert.notNull(action, "Callback object must not be null");
        Assert.notNull(tableName, "No table specified");

        StopWatch sw = new StopWatch();
        sw.start();
        Table table = null;
        try {
            table = this.getConnection().getTable(TableName.valueOf(tableName));
            return action.doInTable(table);
        } catch (Throwable throwable) {
            throw new HbaseSystemException(throwable);
        } finally {
            if (null != table) {
                try {
                    table.close();
                    sw.stop();
                } catch (IOException e) {
                    log.error("Hbase资源释放失败");
                }
            }
        }
    }


}
