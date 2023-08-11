package com.gllis.hbase.api;


import com.gllis.hbase.annotaion.HbaseTable;
import com.gllis.hbase.model.Column;
import com.gllis.hbase.model.RowKey;
import com.gllis.hbase.util.ByteUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * 提供Hbase 查询、新增等操作
 *
 * @author glli
 * @date 2023/8/10
 */
public class HbaseTemplate {

    private HbaseOperations operations;

    public HbaseTemplate(Configuration configuration) {
        operations = new HbaseOperations();
        operations.setConfiguration(configuration);
        Assert.notNull(configuration, "a valid configuration is required");
    }

    /**
     * 保存数据到hbase
     *
     * @param rowKey 主键
     * @param obj
     * @param <T>
     */
    public <T> void put(RowKey rowKey, T obj, Long ttl) {
        Assert.notNull(rowKey, " rowKey not null");
        HbaseTable hbaseTable = obj.getClass().getAnnotation(HbaseTable.class);
        if (hbaseTable == null) {
            return;
        }
        String defaultFamily = hbaseTable.defaultFamily();
        Field[] fields = obj.getClass().getDeclaredFields();
        Map<String, Column> columnMap = operations.getColumnMap(obj.getClass().getName(), fields);
        Map<String, Boolean> columnIgnoreMap = operations.getColumnIgnoreMap(obj.getClass().getName(), fields);

        operations.execute(hbaseTable.value(), (table) -> {
            try {
                Put put = getPut(obj, rowKey, defaultFamily, fields, columnMap, columnIgnoreMap);
                // 单位为天，转换为毫秒
                if (ttl != null && ttl > 0) {
                    put.setTTL(ttl * 86400000);
                }
                table.put(put);
            } catch (Exception e) {
                e.printStackTrace();
            }

            return rowKey;
        });

    }



    /**
     * 删除数据
     *
     * @param tableName
     * @param list
     */
    public void delete(String tableName, List<String> list) {
        operations.execute(tableName, (table -> {
            List<Delete> deleteList = new ArrayList<>();
            for (String rowId : list) {
                Delete delete = new Delete(Bytes.toBytes(rowId));
                deleteList.add(delete);
            }
            table.delete(deleteList);
            return null;
        }));
    }

    /**
     * 查询列表
     *
     * @param startRowKey
     * @param endRowKey
     * @param clazz
     * @return
     */
    public <T> List<T> find(RowKey startRowKey, RowKey endRowKey, Class<T> clazz) {
        Scan scan = new Scan(startRowKey.toBytes(), endRowKey.toBytes());
        HbaseTable hbaseTable = clazz.getAnnotation(HbaseTable.class);
        if (hbaseTable == null) {
            return Collections.emptyList();
        }
        String defaultFamily = hbaseTable.defaultFamily();
        Field[] fields = clazz.getDeclaredFields();
        Map<String, Column> columnMap = operations.getColumnMap(clazz.getName(), fields);
        Map<String, Boolean> columnIgnoreMap = operations.getColumnIgnoreMap(clazz.getName(), fields);
        return operations.execute(hbaseTable.value(), (table) -> {
            ResultScanner scanner = table.getScanner(scan);
            try {
                List<T> rs = new ArrayList<>();
                for (Result result : scanner) {
                    rs.add(getEntity(clazz, defaultFamily, columnMap, columnIgnoreMap, result));
                }
                return rs;
            } finally {
                scanner.close();
            }
        });
    }

    /**
     * 查询指定id对象
     *
     * @param rowKey
     * @param clazz
     * @return
     * @param <T>
     */
    public <T> T findObject(RowKey rowKey, Class<T> clazz) {
        Field[] fields = clazz.getDeclaredFields();
        HbaseTable hbaseTable = clazz.getAnnotation(HbaseTable.class);
        if (hbaseTable == null) {
            return null;
        }
        String defaultFamily = hbaseTable.defaultFamily();
        Map<String, Column> columnMap = operations.getColumnMap(clazz.getName(), fields);
        Map<String, Boolean> columnIgnoreMap = operations.getColumnIgnoreMap(clazz.getName(), fields);
        return operations.execute(hbaseTable.value(), (table) -> {
            Get get = new Get(rowKey.toBytes());
            Result result = table.get(get);
            return getEntity(clazz, defaultFamily, columnMap, columnIgnoreMap, result);
        });

    }

    /**
     * 获取单个对象
     *
     * @param clazz
     * @param defaultFamily
     * @param columnMap
     * @param columnIgnoreMap
     * @param result
     * @return
     */
    private static <T> T getEntity(Class<T> clazz, String defaultFamily, Map<String, Column> columnMap, Map<String, Boolean> columnIgnoreMap, Result result) throws InstantiationException, IllegalAccessException {
        T entity = clazz.newInstance();
        for (Field field : entity.getClass().getDeclaredFields()) {
            field.setAccessible(true);
            int mod = field.getModifiers();
            if (Modifier.isStatic(mod) || Modifier.isFinal(mod)) {
                continue;
            }
            if (columnIgnoreMap.get(field.getName())) {
                continue;
            }
            Column column = columnMap.get(field.getName());
            String family = (column == null || StringUtils.isEmpty(column.getFamily())) ? defaultFamily : column.getFamily();
            String qualifier = (column == null || StringUtils.isEmpty(column.getQualifier())) ? field.getName() : column.getQualifier();

            if (StringUtils.isEmpty(family)) {
                continue;
            }
            if (StringUtils.isEmpty(qualifier)) {
                continue;
            }
            byte[] bytes = result.getValue(Bytes.toBytes(family), Bytes.toBytes(qualifier));
            if (bytes != null) {
                ByteUtil.setField(field, entity, bytes);
            }
        }
        return entity;
    }

    /**
     * 转换成put
     *
     * @param obj
     * @param rowKey
     * @param defaultFamily
     * @param fields
     * @param columnMap
     * @param columnIgnoreMap
     */
    private static <T> Put getPut(T obj, RowKey rowKey, String defaultFamily, Field[] fields, Map<String, Column> columnMap, Map<String, Boolean> columnIgnoreMap) throws IllegalAccessException {
        Put put = new Put(rowKey.toBytes());
        for (Field field : fields) {
            field.setAccessible(true);
            int mod = field.getModifiers();
            if (Modifier.isStatic(mod) || Modifier.isFinal(mod)) {
                continue;
            }
            if (columnIgnoreMap.get(field.getName())) {
                continue;
            }
            Object value = field.get(obj);
            if (value == null) {
                continue;
            }
            Column column = columnMap.get(field.getName());
            String family = (column == null || org.apache.commons.lang3.StringUtils.isEmpty(column.getFamily())) ? defaultFamily : column.getFamily();
            String qualifier = (column == null || org.apache.commons.lang3.StringUtils.isEmpty(column.getQualifier())) ? field.getName() : column.getQualifier();

            put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), ByteUtil.toByte(field.getType(), value));
        }
        return put;
    }
}
