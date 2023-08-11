package com.gllis.hbase.boot;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Hbase配置属性
 *
 * @author glli
 * @date 2023/8/10
 */
@ConfigurationProperties(prefix = "spring.data.hbase")
public class HbaseProperties {
    private String quorum;
    private String rootDir;
    private String nodeParent;
    private String zooDataDir;

    public String getQuorum() {
        return quorum;
    }

    public void setQuorum(String quorum) {
        this.quorum = quorum;
    }

    public String getRootDir() {
        return rootDir;
    }

    public void setRootDir(String rootDir) {
        this.rootDir = rootDir;
    }

    public String getNodeParent() {
        return nodeParent;
    }

    public void setNodeParent(String nodeParent) {
        this.nodeParent = nodeParent;
    }

    public String getZooDataDir() {
        return zooDataDir;
    }

    public void setZooDataDir(String zooDataDir) {
        this.zooDataDir = zooDataDir;
    }
}
