package com.gllis.hbase.util;

import org.apache.hadoop.hbase.util.Bytes;

import java.lang.reflect.Field;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Date;

/**
 * byte转换工具
 *
 * @author glli
 * @date 2023/8/10
 */
public class ByteUtil {

    /**
     * 将对象的值转换成 hbase 数组
     *
     * @param type
     * @param value
     * @return
     */
    public static byte[] toByte(Class<?> type, Object value) {
        if (type == Integer.class) {
            return Bytes.toBytes((Integer) value);
        } else if (type == Long.class) {
            return Bytes.toBytes((Long) value);
        } else if (type == Double.class) {
            return Bytes.toBytes((Double) value);
        } else if (type == Float.class) {
            return Bytes.toBytes((Float) value);
        } else if (type == Short.class) {
            return Bytes.toBytes((Short) value);
        } else if (type == Boolean.class) {
            return Bytes.toBytes((Boolean) value);
        } else if (type == Byte.class) {
            return new byte[]{(Byte) value};
        } else if (type == Date.class) {
            long time = ((Date) value).getTime();
            return Bytes.toBytes(time);
        } else if (type == LocalDateTime.class) {
            long time = ((LocalDateTime) value).toEpochSecond(ZoneOffset.UTC);
            return Bytes.toBytes(time);
        } else {
            return Bytes.toBytes((String) value);
        }
    }

    /**
     * hbase数组 转换成对象值
     *
     * @param field
     * @param obj
     * @param value
     */
    public static void setField(Field field, Object obj, byte[] value) {
        if (obj == null || value == null) {
            return;
        }
        Class<?> type = field.getType();
        try {
            if (type == Integer.class) {
                field.set(obj, Bytes.toInt(value));
            } else if (type == Long.class) {
                field.set(obj, Bytes.toLong(value));
            } else if (type == Double.class) {
                field.set(obj, Bytes.toDouble(value));
            } else if (type == Float.class) {
                field.set(obj, Bytes.toFloat(value));
            } else if (type == Short.class) {
                field.set(obj, Bytes.toShort(value));
            } else if (type == Boolean.class) {
                field.set(obj, Bytes.toBoolean(value));
            } else if (type == Byte.class) {
                field.set(obj, value[0]);
            } else if (type == Date.class) {
                long time = Bytes.toLong(value);
                field.set(obj, new Date(time));
            } else if (type == LocalDateTime.class) {
                long time = Bytes.toLong(value);
                LocalDateTime.ofEpochSecond(time,0, ZoneOffset.ofHours(8));
            } else {
                field.set(obj, new String(value));
            }
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
    }
}
