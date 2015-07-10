package com.meituan.storm.dataservice;

import com.meituan.service.hbase.HBaseClient;
import com.meituan.service.hbase.impl.DefaultHBaseClient;
import com.meituan.storm.common.metrics.api.MTMeanMetric;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * User: qiyiping
 * Date: 14-1-7
 * Time: 下午2:33
 */
public class UserActionHbaseClient {
    private static final Logger logger = LoggerFactory.getLogger(UserActionHbaseClient.class);

    //	private HTable htable;
//    private final String HBASE_ZK_HOST = "data-hbase02,data-hbase03,data-hbase04";
    //private final String HBASE_ZK_HOST = "hadoop-test01,hadoop-test02,hadoop-test03";
//    private final String HBASE_ZK_PORT = "2181";
    HBaseClient client;

    private final String tableName = "user_activity";

    public UserActionHbaseClient() {
        try {
//			Configuration config = HBaseConfiguration.create();
//			// config zookeeper for hbase
//			config.set("hbase.zookeeper.quorum", HBASE_ZK_HOST);
//			config.set("hbase.zookeeper.property.clientPort", HBASE_ZK_PORT);
//
//			UserGroupInformation.setConfiguration(config);
//			UserProvider userProvider = UserProvider.instantiate(config);
//
//			if (userProvider.isHadoopSecurityEnabled() && userProvider.isHBaseSecurityEnabled()) {
//				try {
//					String machineName =
//							Strings.domainNamePointerToHostName(DNS.getDefaultHost("default", "default"));
//					userProvider.login("hbase.app.keytab.file", "hbase.app.kerberos.principal", machineName);
//				} catch (IOException e) {
//					logger.error(e.getMessage(), e);
//				}
//			}

            client = new DefaultHBaseClient();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    public void handleAction(String jsonStr, MTMeanMetric rMetric) {
        try {
            JSONObject jsonObj = new JSONObject(jsonStr);
            String uuid = jsonObj.getString("uuid");
            long actionTime = Long.parseLong(jsonObj.getString("time"));
            String userId = "";
            if (jsonObj.has("userId")) {
                userId = jsonObj.getString("userId");
                insert("cf", "uuid_uid_map", userId, uuid, actionTime);
                insert("cf", "uuid_uid_map", uuid, userId, actionTime);
            }

            if (!jsonObj.has("type")) {
                return;
            }
            String actionType = jsonObj.getString("type");

            String feature = null;
            if (jsonObj.has("feature")) {
                feature = jsonObj.getString("feature");
                insert("cf", actionType, uuid, feature, actionTime);

                if (!userId.isEmpty()) {
                    insert("cf", actionType, userId, feature, actionTime);
                }
            }

            long end = System.currentTimeMillis();
            rMetric.update((end - actionTime) / 1000);

        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            logger.error("failed to handle: " + jsonStr);
        }
    }

    /**
     * insert one key/value to hbase
     *
     * @param familyName      列族标识
     * @param columnQualifier 列标识
     * @param value
     * @throws IOException
     */
    public void insert(String familyName, String columnQualifier,
                       String rowKey, String value, long ts) throws IOException {
        if (rowKey.length() == 0)
            return;
        Put put = new Put(rowKey.getBytes());
        put.add(familyName.getBytes(), columnQualifier.getBytes(), ts, value.getBytes());
        try {
            client.put(tableName, put);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    /**
     * 获取数据，返回String类型
     *
     * @param familyName
     * @param columnQualifier
     * @param rowKey
     * @return
     * @throws IOException
     */
    public String getValue(String familyName, String columnQualifier,
                           String rowKey) throws Exception {
        Get get = new Get(rowKey.getBytes());
        get.addColumn(familyName.getBytes(), columnQualifier.getBytes());
        Result result = null;
        try {
            result = client.get(tableName, get);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            throw new Exception(e);
        }
        return Bytes.toString(result.getValue(familyName.getBytes(), columnQualifier.getBytes()));
    }
}
