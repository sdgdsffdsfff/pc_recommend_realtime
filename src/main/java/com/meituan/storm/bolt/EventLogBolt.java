package com.meituan.storm.bolt;

import com.meituan.storm.common.AbstractStringFilterBolt;
import com.meituan.storm.parser.EventLogParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by kaiserding on 14-12-11.
 */
public class EventLogBolt extends AbstractStringFilterBolt {

    private final EventLogParser _logParser;
    private static final Logger logger = LoggerFactory.getLogger(EventLogBolt.class);

    public EventLogBolt(EventLogParser parser) {
        _logParser = parser;
    }

    protected String filter(String log) {
        String result = _logParser.parse(log, rMetric);
        if (result != null) {
//            logger.info(result);
        }
        return result;
    }

}
