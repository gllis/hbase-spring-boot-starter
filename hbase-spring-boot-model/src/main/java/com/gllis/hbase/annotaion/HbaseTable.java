package com.gllis.hbase.annotaion;


import org.springframework.stereotype.Component;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * hbase 表注解
 *
 * @author glli
 * @date 2023/8/10
 */
@Component
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface HbaseTable {
    // 表名
    String value();
    // 默认family名
    String defaultFamily();
}
