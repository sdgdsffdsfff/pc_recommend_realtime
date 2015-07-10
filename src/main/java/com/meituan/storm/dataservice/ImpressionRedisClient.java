package com.meituan.storm.dataservice;

import com.meituan.storm.common.metrics.api.MTMeanMetric;
import com.meituan.storm.util.RedisUtils;
import com.meituan.storm.util.Utils;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisCluster;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * User: kaiserding
 * Date: 14-12-16
 * Time: 上午11:40
 */
public class ImpressionRedisClient {
    private static final Logger logger = LoggerFactory.getLogger(ImpressionRedisClient.class);

    // redis user action对象池
    private static JedisCluster jedisCluster;
    private static final String[] colnames = {"strategy", "model", "action", "createTime"};

    public ImpressionRedisClient() {
        jedisCluster = RedisUtils.getJedisCluster("redis_impression.properties");
    }

    public void handleAction(String jsonStr, MTMeanMetric rMetric) throws ParseException {
        try {
            JSONObject jsonObj = new JSONObject(jsonStr);

            if (!"getRecommendByDeal".equals(jsonObj.getString("action"))) {
                return;
            }

            JSONObject result = new JSONObject();
            String gid = "app_dataapp_gid_" + jsonObj.getString("gid");
            for (String colname : colnames) {
                result.put(colname, jsonObj.get(colname));
            }

            //更新监控时间
            String createTime = jsonObj.getString("createTime");
            String currentTime = Utils.getCurrentTime("yyyy-MM-dd HH:mm:ss");
            SimpleDateFormat sd = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            Date beginDate = sd.parse(createTime);
            Date endDate = sd.parse(currentTime);
            long time = (endDate.getTime() - beginDate.getTime()) / 1000;
            rMetric.update(time);

            result.put("currentTime", currentTime);
            jedisCluster.setex(gid, 3600, result.toString());

            //logger.info(result.toString());
        } catch (JSONException e) {
            return;
        }
    }
}
