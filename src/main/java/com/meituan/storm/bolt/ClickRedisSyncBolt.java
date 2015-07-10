package com.meituan.storm.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.meituan.storm.common.metrics.api.MTMeanMetric;
import com.meituan.storm.dataservice.ClickRedisClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.util.Map;

/**
 * User: kaiserding
 * Date: 14-6-5
 * Time: 下午2:44
 */
public class ClickRedisSyncBolt extends BaseRichBolt {

    private static final Logger logger = LoggerFactory.getLogger(ClickRedisSyncBolt.class);
    private OutputCollector _collector;
    private ClickRedisClient client;
    public MTMeanMetric rMetric;

    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
        ApplicationContext ctx = new ClassPathXmlApplicationContext("data-service.xml");
        client = (ClickRedisClient) ctx.getBean("clickRedisClient");

        rMetric = new MTMeanMetric();                                       //prepare中生成相应的对象
        context.registerMetric("redisLatency", rMetric, 60);              //将该对象注册到context中，metrics对应的名字"executeLatency"，每隔60s收集一次
    }


    public void execute(Tuple tuple) {

        String jsonStr = tuple.getString(0);
        String result = client.handleAction(jsonStr, rMetric);

        if (result != null) {
            logger.info(result);
            _collector.emit(tuple, new Values(result));
        }

        _collector.ack(tuple);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("click"));
    }
}
