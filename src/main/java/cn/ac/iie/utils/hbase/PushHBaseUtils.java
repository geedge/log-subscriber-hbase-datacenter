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
import java.util.List;
import java.util.Map;

/**
 * HBase 工具类
 *
 * @author qidaijie
 */

public class PushHBaseUtils {
    private final static Logger logger = Logger.getLogger(PushHBaseUtils.class);
    private static Connection connection;
    private static String zookeeperIp;
    private static PushHBaseUtils pushHBaseUtils;

    private static void getHbaseInstance() {
        pushHBaseUtils = new PushHBaseUtils();
    }

    /**
     * 构造函数-新
     */
    private PushHBaseUtils() {
        zookeeperIp = SubscriberConfig.CENTER_HBASE_ZOOKEEPER_SERVERS;
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
     * 写入数据到HBase
     *
     * @param putList puts list
     */
    public static void insertData(List<Put> putList) {
        if (pushHBaseUtils == null) {
            getHbaseInstance();
        }
        Table table = null;
        try {
            table = connection.getTable(TableName.valueOf("sub:" + SubscriberConfig.HBASE_TABLE_NAME));
            table.put(putList);
            logger.warn("同步国家中心HBase增量写入分中心 HBase 成功，更新条数：" + putList.size());
        } catch (IOException e) {
            logger.error("同步国家中心 HBase 增量写入分中心 HBase 失败");
            e.printStackTrace();
        } finally {
            try {
                if (table != null) {
                    table.close();
                }
            } catch (IOException e) {
                logger.error("HBase表关闭异常");
                e.printStackTrace();
            }
        }

    }

}
