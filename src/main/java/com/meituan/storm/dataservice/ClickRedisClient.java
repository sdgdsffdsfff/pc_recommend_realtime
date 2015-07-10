package com.meituan.storm.dataservice;

import com.meituan.storm.common.metrics.api.MTMeanMetric;
import com.meituan.storm.util.RedisUtils;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisCluster;

import java.util.HashMap;

/**
 * User: kaiserding
 * Date: 14-12-16
 * Time: 上午11:40
 */
public class ClickRedisClient {
    private static final Logger logger = LoggerFactory.getLogger(ClickRedisClient.class);

    // redis user action对象池
    private static JedisCluster jedisCluster;

    public ClickRedisClient() {
        jedisCluster = RedisUtils.getJedisCluster("redis_impression.properties");
    }

    public String getField(JSONObject logJO, String fieldName) {
        try {
            return logJO.getString(fieldName);
        } catch (JSONException x) {
            return null;
        }
    }

    public void addOne(HashMap<String, Integer> counter, String key) {
        Integer val = counter.get(key);
        if (val == null) {
            val = 0;
        }
        counter.put(key, ++val);
    }

    public void incCounter(JSONObject info, HashMap counter) {

        String model = getField(info, "model");
        String strategy = getField(info, "strategy");
        String action = getField(info, "action");
        String position = getField(info, "position");

        addOne(counter, "deal.rec.right");
        if (strategy != null && !"".equals(strategy)) {
            addOne(counter, "deal.rec.S." + strategy);
        }
        if (model != null && !"".equals(model)) {
            addOne(counter, "deal.rec.M." + model);
        }
        if (position != null && !"".equals(position)) { // position
            addOne(counter, "deal.rec.P." + position);
        }

    }

    public String handleAction(String jsonStr, MTMeanMetric rMetric) {
        try {
            //logger.info(jsonStr);
            JSONObject jsonObj = new JSONObject(jsonStr);
            String gid = "app_dataapp_gid_" + jsonObj.getString("gid");
            String position = jsonObj.getString("position");

            String impressionString = jedisCluster.get(gid);
            if (impressionString != null) {
                JSONObject info = new JSONObject(impressionString);
                info.put("position", position);

                HashMap<String, Integer> counter = new HashMap();
                incCounter(info, counter);

                JSONObject result = new JSONObject();
                result.put("counter", counter);

                //logger.info(info.toString());
                return result.toString();
            } else {
                //logger.error("cannot find gid:" + gid);
                return null;
            }
        } catch (JSONException e) {
            return null;
        }

    }
}
