package com.meituan.storm.common;

import com.meituan.storm.common.metrics.api.MTMeanMetric;

/**
 * Created by kaiserding on 14-12-12.
 */
public abstract class AbstractLogParser {
    public abstract String parse(String log, MTMeanMetric rMetric);
}
