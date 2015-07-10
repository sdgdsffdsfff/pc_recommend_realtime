package com.meituan.storm.dataservice;

import com.meituan.storm.common.metrics.api.MTMeanMetric;
import com.meituan.storm.util.RedisUtils;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

/**
 * User: zhenggang
 * Date: 14-7-1
 * Time: 下午8:40
 */
public class UserActionRedisClient {
    private static final Logger logger = LoggerFactory.getLogger(UserActionRedisClient.class);

    // redis user action对象池
    private JedisCluster jedisCluster;

    public UserActionRedisClient() {
        jedisCluster = RedisUtils.getJedisCluster("redis_useraction.properties");
    }

    public void handleAction(String jsonStr, MTMeanMetric rMetric) {
        try {
            JSONObject jsonObj = new JSONObject(jsonStr);
            String uuid = jsonObj.getString("uuid");
            long actionTime = Long.parseLong(jsonObj.getString("time"));
            String key = "", value = "";
            String userId = "";
            if (jsonObj.has("userId")) {
                userId = jsonObj.getString("userId");
                key = userId + "_" + "mapping";
                jedisCluster.setex(key, 3600 * 24 * 30, uuid); // keep 30 days

                key = uuid + "_" + "mapping";
                jedisCluster.setex(key, 3600 * 24 * 30, userId); // keep 30 days
            }

            if (!jsonObj.has("type")) {
                return;
            }
            String actionType = jsonObj.getString("type");
            key = uuid;

            String feature = null;
            if (jsonObj.has("feature")) {
                feature = jsonObj.getString("feature");
                value = actionType + ":" + feature + ":" + (actionTime / 60000);

                jedisCluster.rpush(key, value);
                jedisCluster.expire(key, 3600 * 24 * 30);
                jedisCluster.ltrim(key, -1000, -1); // only keep the recent 1000 items

                if (!userId.isEmpty()) {
                    key = userId;
                    jedisCluster.rpush(key, value);
                    jedisCluster.expire(key, 3600 * 24 * 30);
                    jedisCluster.ltrim(key, -1000, -1); // only keep the recent 1000 items
                }
            }

            long end = System.currentTimeMillis();
            rMetric.update((end - actionTime) / 1000);

        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            logger.error("UserActionRedisClient failed to handle: " + jsonStr);
        }
    }
}
