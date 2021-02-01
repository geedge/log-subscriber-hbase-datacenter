package cn.ac.iie.utils.hbase;

import cn.ac.iie.common.SubscriberConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * HBase 工具类
 *
 * @author qidaijie
 */

public class PullHBaseUtils {
    private final static Logger logger = Logger.getLogger(PullHBaseUtils.class);
    private static Map<String, String> subIdMap = new HashMap<>(83334);
    private static Connection connection;
    private static Long time;

    private static String zookeeperIp;
    private static String hBaseTable;

    private static PullHBaseUtils pullHBaseUtils;

    private static void getHbaseInstance() {
        pullHBaseUtils = new PullHBaseUtils();
    }


    /**
     * 构造函数-新
     */
    private PullHBaseUtils() {
        zookeeperIp = SubscriberConfig.NATIONAL_HBASE_ZOOKEEPER_SERVERS;
        hBaseTable = SubscriberConfig.HBASE_TABLE_NAME;
        //获取连接
        getHbaseConn();
    }

    private static void getHbaseConn() {
        try {
            // 管理Hbase的配置信息
            Configuration configuration = HBaseConfiguration.create();
            // 设置zookeeper节点
            configuration.set("hbase.zookeeper.quorum", zookeeperIp);
            configuration.set("hbase.client.retries.number", "3");
            configuration.set("hbase.bulkload.retries.number", "3");
            configuration.set("zookeeper.recovery.retry", "3");
            connection = ConnectionFactory.createConnection(configuration);
            time = System.currentTimeMillis();
            logger.warn("PullHBaseUtils get HBase connection,now to getAll().");
        } catch (IOException ioe) {
            logger.error("PullHBaseUtils getHbaseConn() IOException===>{" + ioe + "}<===");
            ioe.printStackTrace();
        } catch (Exception e) {
            logger.error("PullHBaseUtils getHbaseConn() Exception===>{" + e + "}<===");
            e.printStackTrace();
        }
    }

    /**
     * 更新变量
     */
    public static void change(Map<String, String> hashMap) {
        if (pullHBaseUtils == null) {
            getHbaseInstance();
        }
        long nowTime = System.currentTimeMillis();
        timestampsFilter(time - 1000, nowTime + 500);
        hashMap.putAll(subIdMap);
        subIdMap.clear();
    }


    /**
     * 获取变更内容
     *
     * @param startTime 开始时间
     * @param endTime   结束时间
     */
    private static void timestampsFilter(Long startTime, Long endTime) {
        Long begin = System.currentTimeMillis();
        Table table = null;
        ResultScanner scanner = null;
        Scan scan2 = new Scan();
        try {
            table = connection.getTable(TableName.valueOf("sub:" + hBaseTable));
            scan2.setTimeRange(startTime, endTime);
            scanner = table.getScanner(scan2);
            for (Result result : scanner) {
                Cell[] cells = result.rawCells();
                for (Cell cell : cells) {
                    String key = Bytes.toString(CellUtil.cloneRow(cell));
                    String value = Bytes.toString(CellUtil.cloneValue(cell));
                    if (subIdMap.containsKey(key)) {
                        if (!value.equals(subIdMap.get(key))) {
                            subIdMap.put(key, value);
                        }
                    } else {
                        subIdMap.put(key, value);
                    }
                }
            }
            Long end = System.currentTimeMillis();
            logger.warn("PullHBaseUtils Now subIdMap.keySet().size() is: " + subIdMap.keySet().size());
            logger.warn("PullHBaseUtils Update cache timeConsuming is: " + (end - begin) + ",BeginTime: " + begin + ",EndTime: " + end);
            time = endTime;
        } catch (IOException ioe) {
            logger.error("PullHBaseUtils timestampsFilter is IOException===>{" + ioe + "}<===");
            ioe.printStackTrace();
        } catch (Exception e) {
            logger.error("PullHBaseUtils timestampsFilter is Exception===>{" + e + "}<===");
            e.printStackTrace();
        } finally {
            if (scanner != null) {
                scanner.close();
            }
            if (table != null) {
                try {
                    table.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }


}
