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
import java.util.Properties;

/**
 * 实时将blog里uuid和userid的对应关系保存到redis里
 *
 * @author zhangbo09
 * @version 1.0
 * @created 2014-06-05
 */
public class UUIDUserIdRedisClient {

    private static final Logger logger = LoggerFactory.getLogger(UUIDUserIdRedisClient.class);

    // redis对象池池
    private static JedisPool jedisPool;

    public UUIDUserIdRedisClient() {
        jedisPool = RedisUtils.getJedisPool("redis_ad.properties");
    }

    public void handleAction(String jsonStr) {
        Jedis jedis = null;
        try {
            JSONObject jsonObj = new JSONObject(jsonStr);
            String uuid = jsonObj.getString("uuid");
            String userId = "";
            if (jsonObj.has("userId")) {
                userId = jsonObj.getString("userId");
                jedis = jedisPool.getResource();
                jedis.set(uuid, userId);
            }
        } catch (JedisConnectionException je) {
            if (jedis != null) {
                jedisPool.returnBrokenResource(jedis);
                jedis = null;
            }
            logger.error(je.getMessage(), je);
            logger.error("failed to handle: " + jsonStr);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            logger.error("failed to handle: " + jsonStr);
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }
        }
    }
}
