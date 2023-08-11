package com.gllis.hbase.model;


/**
 * 列名信息
 *
 * @author glli
 * @date 2023/8/10
 */
public class Column {

    private String family;
    private String qualifier;

    public Column() {
    }

    public Column(String family, String qualifier) {
        this.family = family;
        this.qualifier = qualifier;
    }

    public String getFamily() {
        return family;
    }

    public void setFamily(String family) {
        this.family = family;
    }

    public String getQualifier() {
        return qualifier;
    }

    public void setQualifier(String qualifier) {
        this.qualifier = qualifier;
    }
}
