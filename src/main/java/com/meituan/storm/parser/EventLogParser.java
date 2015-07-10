package com.meituan.storm.parser;

import com.meituan.storm.bolt.EventLogBolt;
import com.meituan.storm.common.AbstractLogParser;
import com.meituan.storm.common.metrics.api.MTMeanMetric;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

/**
 * Created by kaiserding on 14-12-12.
 */
public class EventLogParser extends AbstractLogParser implements Serializable {

    private static final Logger logger = LoggerFactory.getLogger(EventLogBolt.class);

    public String getField(JSONObject logJO, String fieldName) {
        try {
            return logJO.getString(fieldName);
        } catch (JSONException x) {
            return null;
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
        String urlinfo_reccacm = getField(logJSON, "urlinfo_reccacm");

        String gid = null;
        String position = null;
        if (urlinfo_reccacm != null && !"".equals(urlinfo_reccacm)) {
            String[] acmSplits = urlinfo_reccacm.split("\\.");
            String acm = acmSplits[0];
            if (acmSplits.length == 3) {
                position = acmSplits[2];
            } else {
                //只保留三段的log,否则直接返回
                return null;
            }

            int BIndex = acm.indexOf("B");
            if (BIndex != -1) {
                gid = acm.substring(BIndex + 1, acm.length());

            } else {
                int CIndex = acm.indexOf("C");
                gid = acm.substring(CIndex + 1, acm.length());
            }
        }

        if (gid != null) {
            try {
                result.put("gid", gid);
                result.put("position", position);
                return result.toString();
            } catch (JSONException e) {
                e.printStackTrace();
                return null;
            }
        } else {
            return null;
        }
    }
}
