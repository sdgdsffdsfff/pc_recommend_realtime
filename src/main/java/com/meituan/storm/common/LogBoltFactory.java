package com.meituan.storm.common;

import com.meituan.storm.bolt.BLogBolt;
import com.meituan.storm.bolt.EventLogBolt;
import com.meituan.storm.bolt.ResysLogBolt;
import com.meituan.storm.parser.BLogParser;
import com.meituan.storm.parser.EventLogParser;
import com.meituan.storm.parser.ResysLogParser;

/**
 * Created by kaiserding on 14-12-15.
 */
public class LogBoltFactory {
    public AbstractStringFilterBolt createBolt(String boltNamePrefix) {
        if ("BLog".equals(boltNamePrefix)) {
            return new BLogBolt(new BLogParser());
        } else if ("EventLog".equals(boltNamePrefix)) {
            return new EventLogBolt(new EventLogParser());
        } else if ("ResysLog".equals(boltNamePrefix)) {
            return new ResysLogBolt(new ResysLogParser());
        } else {
            return null;
        }
    }
}
