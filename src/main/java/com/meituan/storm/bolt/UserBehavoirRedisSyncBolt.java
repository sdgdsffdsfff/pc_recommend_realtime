package com.meituan.storm.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import com.meituan.storm.common.metrics.api.MTMeanMetric;
import com.meituan.storm.dataservice.UserActionHbaseClient;
import com.meituan.storm.dataservice.UserActionRedisClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.util.Map;

/**
 * User: zhenggang
 * Date: 14-7-1
 * Time: 下午8:40
 */
public class UserBehavoirRedisSyncBolt extends BaseRichBolt {
    private static final Logger logger = LoggerFactory.getLogger(UserBehavoirRedisSyncBolt.class);


    private OutputCollector _collector;

    private UserActionRedisClient client;

	public MTMeanMetric rMetric;                                           //增加一个求平均数的metrics对象

	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        _collector = collector;

        ApplicationContext ctx = new ClassPathXmlApplicationContext("data-service.xml");
        client = (UserActionRedisClient) ctx.getBean("userActionRedisClient");

		rMetric = new MTMeanMetric();                                       //prepare中生成相应的对象
		context.registerMetric("redisLatency", rMetric, 60);              //将该对象注册到context中，metrics对应的名字"executeLatency"，每隔60s收集一次
    }


    public void execute(Tuple tuple) {
        try {
            String jsonStr = tuple.getString(0);
            client.handleAction(jsonStr, rMetric);
        } catch (Exception e) {
            // do nothing
        }

        _collector.ack(tuple);
    }


    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }
}
