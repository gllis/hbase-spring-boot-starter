# Hbase Spring Boot Start

## usage 


### pom引入
```xml
<dependency>
    <groupId>com.gllis</groupId>
    <artifactId>hbase-spring-boot-start</artifactId>
    <version>1.0</version>
</dependency>
```

### 配置 springboot 
spring.data.hbase.quorum=hbase01,hbase01,hbase01
spring.data.hbase.rootDir=hdfs://hbase01:2181/hbase
spring.data.hbase.zooDataDir=/opt/data/install/zookeeper-3.6.3/data/hbase

### 使用
```java
@HbaseTable(value = "track", defaultFamily="info")
public class Track {
    private Long deviceId;
    private Double lat;
    private Double lon;
    private Date created;
    ...
}

public class TrackRowKey implements RowKey {
    private String rowKey;
    
    public TrackRowkey(Track track) {
        this.rowKey = track.getDeviceId() + "-" + DateFormat.format(track.getCreated());
    }

    public static TrackRowkey create(Track track) {
        return new TrackRowkey(track);
    }
    @Override
    public byte[] toBytes() {
        return Bytes.toBytes(rowKey);
    }
}
```

```java


@Service
public class TrackService {
    @Autowired
    private HbaseTemplate hbaseTemplate;


    public void save(Track track) {
        hbaseTemplate.put(TrackRowKey.create(track), track);
    }

    public List<Track> findList(RowKey startRowKey, RowKey endRowKey) {
        return hbaseTemplate.findList(startRowKey, endRowKey, Track.class);
    }

    public void delete(Track track) {
        hbaseTemplate.deleteObject(TrackRowKey.create(track), Track.clsss);
    }
}
```

### 自定义查询
```java
public <T> List<T> find(Scan scan, Class<T> clazz);
```
