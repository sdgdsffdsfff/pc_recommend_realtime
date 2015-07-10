package com.meituan.storm.common;

import java.util.Map;

import backtype.storm.task.TopologyContext;
import backtype.storm.task.OutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;

import com.meituan.storm.common.metrics.api.MTMeanMetric;

public abstract class AbstractStringFilterBolt extends BaseRichBolt {
	private static final long serialVersionUID = -8086797098054084059L;
	
	//private static final Logger LOG = LoggerFactory.getLogger(AbstractStringFilterBolt.class);

	private OutputCollector _collector;

	public MTMeanMetric rMetric;                                           //增加一个求平均数的metrics对

	public AbstractStringFilterBolt() {

    }
    
    public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, OutputCollector outputCollector) {
		_collector = outputCollector;

		rMetric = new MTMeanMetric();                                       //prepare中生成相应的对象
		context.registerMetric("executeLatency", rMetric, 60);              //将该对象注册到context中
    }

	public void execute(Tuple tuple) {
		String jsonStr = filter(tuple.getString(0));

		if(jsonStr != null) {
			_collector.emit(tuple, new Values(jsonStr));
		}
			
		_collector.ack(tuple);
	}

	abstract protected String filter(String log);

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    	declarer.declare(new Fields("filtered_log"));
    }

}
