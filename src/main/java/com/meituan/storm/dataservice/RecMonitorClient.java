/*
 * Copyright (c) 2010-2012 meituan.com
 * All rights reserved.
 * 
 */
package com.meituan.storm.dataservice;

import com.meituan.storm.util.RedisUtils;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.io.IOException;
import java.io.InputStream;
import java.util.Date;
import java.util.Iterator;
import java.util.Properties;

/**
 * TODO 在这里编写类的功能描述
 *
 * @author zhenggang
 * @version 1.0
 * @created 2014-03-20
 */
public class RecMonitorClient {
    private static final Logger logger = LoggerFactory.getLogger(RecMonitorClient.class);

    // redis对象池池
    private static JedisPool jedisPool;

    public RecMonitorClient() {
        jedisPool = RedisUtils.getJedisPool("redis.properties");
    }

    public void handleAction(String jsonStr) {
        Jedis jedis = null;
        try {
            JSONObject jsonObj = new JSONObject(jsonStr);
            if (!jsonObj.has("counter")) {
                return;
            }
            JSONObject counter = (JSONObject) jsonObj.get("counter");
            Long ts = (new Date()).getTime() / 1000;
            String key_min = "counter.min." + (ts / 60);
            String key_day = "counter.day." + (ts / 86400);
            String click_3min = "clicks.3min." + (ts / 180);
            String click_day = "clicks.day." + (ts / 86400);

            jedis = jedisPool.getResource();
            //Pipeline pipeline = jedis.pipelined();

            for (Iterator<String> it = counter.keys(); it.hasNext(); ) {
                String name = it.next();
                Integer value = (Integer) counter.get(name);

                //logger.info("key_min:" + key_min + ", name:" + name + ", value:" + value);
                //logger.info("key_day:" + key_day + ", name:" + name + ", value:" + value);
                //pipeline.hincrBy(key_min, name, value);
                //pipeline.hincrBy(key_day, name, value);
                jedis.hincrBy(key_min, name, value);
                jedis.hincrBy(key_day, name, value);

                /*if (name.contains(".M.") || name.contains(".Mb.") || name.contains(".S.") || name.contains(".Sb.")) {
                    String n = name.substring(name.lastIndexOf(".") + 1);
                    //pipeline.hincrBy(click_3min, n, value);
                    //pipeline.hincrBy(click_day, n, value);
                    jedis.hincrBy(click_3min, n, value);
                    jedis.hincrBy(click_day, n, value);

                    //logger.info("click_3min:" + click_3min + ", n:" + n + ", value:" + value);
                    //logger.info("click_day:" + click_day + ", n:" + n + ", value:" + value);
                }*/

                if (name.contains("shoppingmall")) {
                    //pipeline.hincrBy(click_3min, name, value);
                    //pipeline.hincrBy(click_day, name, value);
                    jedis.hincrBy(click_3min, name, value);
                    jedis.hincrBy(click_day, name, value);

                    //logger.info("click_3min:" + click_3min + ", name:" + name + ", value:" + value);
                    //logger.info("click_day:" + click_day + ", name:" + name + ", value:" + value);
                }
            }

            //pipeline.expire(key_min, 3600 * 24 * 10);
            //pipeline.expire(key_day, 3600 * 24 * 10);  // keep ten days
            //pipeline.expire(click_3min, 3600 * 24 * 10);  // keep ten days
            //pipeline.expire(click_day, 3600 * 24 * 10);  // keep ten days

            //pipeline.exec();
            jedis.expire(key_min, 3600 * 24 * 10);
            jedis.expire(key_day, 3600 * 24 * 10);  // keep ten days
            jedis.expire(click_3min, 3600 * 24 * 10);  // keep ten days
            jedis.expire(click_day, 3600 * 24 * 10);  // keep ten days

        } catch (JedisConnectionException je) {
            if (jedis != null) {
                jedisPool.returnBrokenResource(jedis);
                jedis = null;
            }
            logger.error(je.getMessage(), je);
            logger.error("failed to handle: " + jsonStr);
        } catch (Exception e) {
            StackTraceElement[] stackTraceElements = e.getStackTrace();
            for (StackTraceElement stackTrace : stackTraceElements) {
                logger.error(stackTrace.getClassName() + "  " + stackTrace.getMethodName() + " " + stackTrace.getLineNumber());
            }
            logger.error("failed to handle: " + jsonStr);
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }
        }
    }
}
