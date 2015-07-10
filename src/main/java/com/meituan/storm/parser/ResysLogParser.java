package com.meituan.storm.parser;

import com.meituan.storm.bolt.ResysLogBolt;
import com.meituan.storm.common.AbstractLogParser;
import com.meituan.storm.common.metrics.api.MTMeanMetric;
import com.meituan.storm.util.Utils;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by kaiserding on 14-12-12.
 */
public class ResysLogParser extends AbstractLogParser implements Serializable {

    private static final Logger logger = LoggerFactory.getLogger(ResysLogBolt.class);

    public String getField(JSONObject logJSON, String fieldName) {
        try {
            return logJSON.getString(fieldName);
        } catch (JSONException x) {
            //logger.error("getfiled error:" + fieldName);
            return "";
        }
    }

    @Override
    public String parse(String log, MTMeanMetric rMetric) {
        JSONObject logJSON = null;
        try {
            logJSON = new JSONObject(log);
        } catch (JSONException jsone) {
            logger.error(jsone.toString());
            return null;
        }
        JSONObject result = new JSONObject();

        //获取gid
        String gid = getField(logJSON, "gid");
        String strategy = getField(logJSON, "strategy");
        String model = getField(logJSON, "model");
        String action = getField(logJSON, "_mt_action");
        String createTime = getField(logJSON, "_mt_datetime");

        //更新监控数据
        SimpleDateFormat sd = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        long beginTime = 0;
        try {
            beginTime = sd.parse(createTime).getTime();

        } catch (ParseException e) {
            e.printStackTrace();
        }
        long endTime = System.currentTimeMillis();
        long time = (endTime - beginTime) / 1000;
        rMetric.update(time);

        if (!"".equals(gid)) {
            try {
                result.put("gid", gid);
                result.put("strategy", strategy);
                result.put("model", model);
                result.put("action", action);
                result.put("createTime", createTime);
                return result.toString();
            } catch (JSONException e) {
                //e.printStackTrace();
                return null;
            }
        } else {
            return null;
        }

    }
}
