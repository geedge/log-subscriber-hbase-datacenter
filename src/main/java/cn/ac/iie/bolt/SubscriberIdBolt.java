package cn.ac.iie.bolt;

import cn.ac.iie.common.SubscriberConfig;
import cn.ac.iie.utils.TupleUtils;
import cn.ac.iie.utils.hbase.PullHBaseUtils;
import cn.ac.iie.utils.hbase.PushHBaseUtils;
import org.apache.hadoop.hbase.client.*;
import org.apache.log4j.Logger;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

import java.util.*;

/**
 * @author qidaijie
 */
public class SubscriberIdBolt extends BaseBasicBolt {
    private static Logger logger = Logger.getLogger(SubscriberIdBolt.class);
    private static Map<String, String> subIdMap;
    private List<Put> putList;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        subIdMap = new HashMap<>(83334);
        putList = new ArrayList<>();
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        try {
            if (TupleUtils.isTick(tuple)) {
                PullHBaseUtils.change(subIdMap);
                for (String key : subIdMap.keySet()) {
                    Put put = new Put(key.getBytes());
                    put.addColumn("subscriber_id".getBytes(), "account".getBytes(), subIdMap.get(key).getBytes());
                    putList.add(put);
                }
                PushHBaseUtils.insertData(putList);
                putList.clear();
                subIdMap.clear();
            } else {
                logger.warn(tuple.getString(0));
            }
        } catch (Exception e) {
            logger.error("获取国家中心HBase Radius 写入分中心 HBase 异常");
            e.printStackTrace();
        }
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Map<String, Object> conf = new HashMap<String, Object>(16);
        conf.put(org.apache.storm.Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS,
                SubscriberConfig.TOPOLOGY_TICK_TUPLE_FREQ_SECS);
        return conf;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}
