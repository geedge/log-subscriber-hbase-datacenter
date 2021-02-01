package cn.ac.iie.common;

import cn.ac.iie.utils.SubscriberConfigurations;

import java.io.Serializable;

/**
 * @author qidaijie
 */
public class SubscriberConfig implements Serializable {
    private static final long serialVersionUID = -8326385159484059324L;

    public static final int LIST_SIZE_MAX = 5000;

    /***
     * kafka and system
     */
    public static final String BOOTSTRAP_SERVERS = SubscriberConfigurations.getStringProperty(0, "bootstrap.servers");
    public static final Integer SPOUT_PARALLELISM = SubscriberConfigurations.getIntProperty(0, "spout.parallelism");
    public static final Integer FORMAT_BOLT_PARALLELISM = SubscriberConfigurations.getIntProperty(0, "format.bolt.parallelism");
    public static final String GROUP_ID = SubscriberConfigurations.getStringProperty(0, "group.id");
    public static final String KAFKA_TOPIC = SubscriberConfigurations.getStringProperty(0, "kafka.topic");
    public static final String AUTO_OFFSET_RESET = SubscriberConfigurations.getStringProperty(0, "auto.offset.reset");
    public static final Integer TOPOLOGY_TICK_TUPLE_FREQ_SECS = SubscriberConfigurations.getIntProperty(0, "topology.tick.tuple.freq.secs");
    public static final Integer TOPOLOGY_CONFIG_MAX_SPOUT_PENDING = SubscriberConfigurations.getIntProperty(0, "topology.config.max.spout.pending");
    public static final Integer TOPOLOGY_NUM_ACKS = SubscriberConfigurations.getIntProperty(0, "topology.num.acks");

    public static final Integer TOPOLOGY_WORKERS = SubscriberConfigurations.getIntProperty(0, "topology.workers");

    public static final String CHECK_IP_SCOPE = SubscriberConfigurations.getStringProperty(0, "check.ip.scope");

    public static final String CENTER_HBASE_ZOOKEEPER_SERVERS = SubscriberConfigurations.getStringProperty(0, "center.hbase.zookeeper.servers");
    public static final String HBASE_TABLE_NAME = SubscriberConfigurations.getStringProperty(0, "hbase.table.name");

    public static final String NATIONAL_HBASE_ZOOKEEPER_SERVERS = SubscriberConfigurations.getStringProperty(0, "national.hbase.zookeeper.servers");
}