package com.meituan.storm.bolt;

import com.meituan.storm.common.AbstractStringFilterBolt;
import com.meituan.storm.parser.BLogParser;

public class BLogBolt extends AbstractStringFilterBolt {
	private static final long serialVersionUID = -8583936155973524773L;
	
	private final BLogParser _logParser;

	public BLogBolt(BLogParser logParser) {
		_logParser = logParser;
	}

	protected String filter(String log) {
		return _logParser.parse(log, rMetric);
	}
}
