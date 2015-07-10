package com.meituan.storm.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import com.meituan.storm.dataservice.RecModelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.util.Map;

public class RecModelBolt extends BaseRichBolt {
    private static final Logger logger = LoggerFactory.getLogger(RecModelBolt.class);

    private OutputCollector _collector;

	private RecModelClient client;

    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        _collector = collector;

		ApplicationContext ctx = new ClassPathXmlApplicationContext("data-service.xml");
		client = (RecModelClient) ctx.getBean("recModelClient");
    }


    public void execute(Tuple tuple) {
        try {
            String jsonStr = tuple.getString(0);
			client.handleAction(jsonStr);
        } catch (Exception e) {
            // do nothing
        }

        _collector.ack(tuple);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("result"));
    }
}
