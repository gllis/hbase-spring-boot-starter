package com.gllis.hbase.boot;

import com.gllis.hbase.api.HbaseTemplate;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Hbase 自动配置
 *
 * @author glli
 * @date 2023/8/10
 */
@Configuration
@EnableConfigurationProperties(HbaseProperties.class)
public class HbaseAutoConfiguration {
    private static final String HBASE_QUORUM = "hbase.zookeeper.quorum";
    private static final String HBASE_ROOT_DIR = "hbase.rootdir";
    private static final String HBASE_ZNODE_PARENT = "zookeeper.znode.parent";
    private static final String HBASE_ZOOKEEPER_PROPERTY_DATADIR = "hbase.zookeeper.property.dataDir";

    @Autowired
    private HbaseProperties hbaseProperties;

    @Bean
    @ConditionalOnMissingBean(HbaseTemplate.class)
    public HbaseTemplate hbaseTemplate() {
        org.apache.hadoop.conf.Configuration configuration = HBaseConfiguration.create();
        configuration.set(HBASE_ROOT_DIR, hbaseProperties.getRootDir());
        configuration.set(HBASE_QUORUM, hbaseProperties.getQuorum());
        if (StringUtils.isNotEmpty(hbaseProperties.getNodeParent())) {
            configuration.set(HBASE_ZNODE_PARENT, hbaseProperties.getNodeParent());
        }
        if (StringUtils.isNotEmpty(hbaseProperties.getZooDataDir())) {
            configuration.set(HBASE_ZOOKEEPER_PROPERTY_DATADIR, hbaseProperties.getZooDataDir());
        }
        return new HbaseTemplate(configuration);
    }
}
