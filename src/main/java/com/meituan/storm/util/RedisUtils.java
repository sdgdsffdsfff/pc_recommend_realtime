package com.meituan.storm.util;

import com.meituan.storm.dataservice.RecMonitorClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;


public class RedisUtils {
	
	private static final Logger logger = LoggerFactory.getLogger(RedisUtils.class);

    public static JedisCluster getJedisCluster(String fileName) {
        InputStream is = RedisUtils.class.getResourceAsStream("/" + fileName);
        Properties properties = new Properties();
        JedisCluster jedisCluster = null;
        try {
            properties.load(is);

            Set<HostAndPort> jedisClusterNodes = new HashSet<HostAndPort>();
            String[] hosts = properties.getProperty("redis.hostAndPorts").split(",");
            for (String host : hosts) {
                String[] hostAndPort = host.split(":");
                jedisClusterNodes.add(new HostAndPort(hostAndPort[0], Integer.parseInt(hostAndPort[1])));
            }

            jedisCluster =  new JedisCluster(jedisClusterNodes,
                    Integer.parseInt(properties.getProperty("redis.timeout")));

        } catch (IOException ioe) {
            logger.error(ioe.toString());
        } finally {
            try {
                is.close();
            } catch (IOException ioe) {
                logger.error(ioe.toString());
            }
            return jedisCluster;
        }
    }

    public static JedisPool getJedisPool(String fileName) {
        InputStream is = RedisUtils.class.getResourceAsStream("/" + fileName);
        Properties properties = new Properties();
        JedisPool jedisPool = null;
        try {
            properties.load(is);

            JedisPoolConfig config = new JedisPoolConfig();

            jedisPool = new JedisPool(config,
                    properties.getProperty("redis.host"),
                    Integer.parseInt(properties.getProperty("redis.port")),
                    Integer.parseInt(properties.getProperty("redis.socket_timeout")),
                    null,
                    Integer.parseInt(properties.getProperty("redis.db")));
        } catch (IOException ioe) {
            logger.error(ioe.toString());
        } finally {
            try {
                is.close();
            } catch (IOException ioe) {
                logger.error(ioe.toString());
            }
            return jedisPool;
        }
    }
}
