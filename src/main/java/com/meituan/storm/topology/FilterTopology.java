package com.meituan.storm.topology;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import com.meituan.storm.common.AbstractStringFilterBolt;
import com.meituan.storm.common.LogBoltFactory;
import com.meituan.storm.common.metrics.MTMetricsConsumer;
import com.meituan.storm.util.Utils;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;

import java.util.ArrayList;

public class FilterTopology {
    private static final Logger LOG = LoggerFactory.getLogger(FilterTopology.class);

    private static final String DEFAULT_TOPOLOGY_CONFIG_FILE = "/app_pc_recommend_realtime.json";
    private String _configFile;

    protected boolean _runMode;
    protected boolean _debugMode;
    protected String _topologyName;
    protected int _workerNum;
    protected String _kafkaZkServers;
    protected String _kafkaZkBrokerPath;
    protected String _kafkaZkRoot;
    protected KafkaTopic[] _kafkaTopics;
    protected BoltInfo[] _boltInfos;

    public FilterTopology() {
    }

    public FilterTopology(String configFile) {
        _configFile = configFile;
    }

    public final boolean getRunMode() {
        return _runMode;
    }

    public final boolean getDebugMode() {
        return _debugMode;
    }

    public final String getTopologyName() {
        return _topologyName;
    }

    public final int getWorkerNum() {
        return _workerNum;
    }

    protected static class KafkaTopic {
        private final String _topicName;
        private final int _startOffset;
        private final String _groupId;

        private final int _spoutExecutorNum;

        private final BoltInfo _boltInfo;

        public KafkaTopic(String topicName, int startOffset, String groupId, int spoutExecutorNum,
                          BoltInfo boltInfo) {
            _topicName = topicName;
            _startOffset = startOffset;
            _groupId = groupId;

            _spoutExecutorNum = spoutExecutorNum;

            _boltInfo = boltInfo;
        }

        public final String getTopicName() {
            return _topicName;
        }

        public final int getStartOffset() {
            return _startOffset;
        }

        public final String getGroupId() {
            return _groupId;
        }

        public final int getSpoutExecutorNum() {
            return _spoutExecutorNum;
        }

        public final BoltInfo getBoltInfo() {
            return _boltInfo;
        }
    }

    protected static class BoltInfo {
        private final String _componentId;
        private final String _boltNamePrefix;
        private final int _boltExecutorNum;

        private final ArrayList<String> _dataSource;

        public BoltInfo(String componentId, String boltNamePrefix, int boltExecutorNum) {
            _componentId = componentId;
            _boltNamePrefix = boltNamePrefix;
            _boltExecutorNum = boltExecutorNum;

            _dataSource = new ArrayList<String>();
        }

        public void addDataSource(String dataSource) {
            _dataSource.add(dataSource);
        }

        public final String getComponentId() {
            return _componentId;
        }

        public final String getBoltNamePrefix() {
            return _boltNamePrefix;
        }

        public final int getBoltExecutorNum() {
            return _boltExecutorNum;
        }

        public final ArrayList<String> getDataSource() {
            return _dataSource;
        }
    }

    public void loadConfigurations() {
        String config = (_configFile == null) ? Utils.loadJsonFileFromJarPackage(DEFAULT_TOPOLOGY_CONFIG_FILE)
                : Utils.loadJsonFileFromLocalFileSystem(_configFile);

        JSONObject jsonObj = null;
        try {
            jsonObj = new JSONObject(config);
            // read topology global configurations
            _runMode = jsonObj.getBoolean("run_local_mode");
            _debugMode = jsonObj.getBoolean("run_debug_mode");
            _topologyName = jsonObj.getString("topology_name");
            _workerNum = jsonObj.getInt("topology_num_workers");

            // read kafka server configurations
            _kafkaZkServers = jsonObj.getString("kafka_zk_servers");
            _kafkaZkBrokerPath = jsonObj.getString("kafka_zk_broker_path");
            _kafkaZkRoot = jsonObj.getString("zk_root");

            // read kafka topics configurations
            JSONArray topics = jsonObj.getJSONArray("kafka_topics");
            int topicNum = topics.length();
            _kafkaTopics = new KafkaTopic[topicNum];
            for (int i = 0; i < topicNum; ++i) {
                JSONObject topic = topics.getJSONObject(i);
                String topicName = topic.getString("topic_name");
                int startOffset = topic.getInt("start_offset");
                String groupId = topicName + "__" + _topologyName;

                int spoutExecutorNum = topic.getInt("spout_num_executors");

                BoltInfo boltInfo = null;
                String boltNamePrefix = topic.getString("bolt_name_prefix");
                if (boltNamePrefix != null) {
                    // here we replace all dots in topicName with underscores, and use it as the bolt's component id
                    String componentId = topicName.replace('.', '_');
                    int boltExecutorNum = topic.getInt("bolt_num_executors");
                    boltInfo = new BoltInfo(componentId, boltNamePrefix, boltExecutorNum);
                    boltInfo.addDataSource(topicName);
                }

                KafkaTopic kafkaTopic = new KafkaTopic(topicName, startOffset, groupId,
                        spoutExecutorNum, boltInfo);
                _kafkaTopics[i] = kafkaTopic;
            }

            // read bolts configurations
            JSONArray bolts = jsonObj.getJSONArray("bolts");
            if (bolts != null) {
                int boltNum = bolts.length();
                _boltInfos = new BoltInfo[boltNum];
                for (int i = 0; i < boltNum; ++i) {
                    JSONObject boltJO = bolts.getJSONObject(i);
                    BoltInfo boltInfo = new BoltInfo(boltJO.getString("component_id"),
                            boltJO.getString("bolt_name_prefix"), boltJO.getInt("bolt_num_executors"));

                    String dataSource = boltJO.getString("data_source");
                    int start, end;
                    for (start = 0; (end = dataSource.indexOf(',', start)) != -1; start = end + 1)
                        boltInfo.addDataSource(dataSource.substring(start, end).replace('.', '_'));

                    boltInfo.addDataSource(dataSource.substring(start).replace('.', '_'));

                    _boltInfos[i] = boltInfo;
                }
            }
        } catch (JSONException jsone) {
            LOG.error(jsone.toString());
            return;
        }
    }

    protected void configSpoutsAndFilterBolts(TopologyBuilder builder) {
        for (KafkaTopic kafkaTopic : _kafkaTopics) {
            String topicName = kafkaTopic.getTopicName();

            // create spouts, one spout per Kafka topic         
            SpoutConfig spoutConfig = new SpoutConfig(
                    new ZkHosts(_kafkaZkServers, _kafkaZkBrokerPath),
                    topicName,
                    _kafkaZkRoot,
                    kafkaTopic.getGroupId());
            spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
            if (kafkaTopic.getStartOffset() >= -2) {
                //spoutConfig.forceStartOffsetTime(kafkaTopic.getStartOffset());
                spoutConfig.forceFromStart = true;
                spoutConfig.startOffsetTime = kafkaTopic.getStartOffset();
            }
            KafkaSpout topicSpout = new KafkaSpout(spoutConfig);
            builder.setSpout(topicName, topicSpout, kafkaTopic.getSpoutExecutorNum());

            BoltInfo boltInfo = kafkaTopic.getBoltInfo();
            if (boltInfo != null) {
                // create filter bolts, one bolt per spout
                String boltNamePrefix = boltInfo.getBoltNamePrefix();
                LogBoltFactory factory = new LogBoltFactory();
                AbstractStringFilterBolt logBolt = factory.createBolt(boltNamePrefix);
                if (logBolt != null) {
                    builder.setBolt(boltInfo.getComponentId(), logBolt, boltInfo.getBoltExecutorNum())
                            .shuffleGrouping(boltInfo.getDataSource().get(0));
                }
            }
        }
    }

    protected void configProcessBolts(TopologyBuilder builder) {
        for (BoltInfo boltInfo : _boltInfos) {
            String boltName = boltInfo.getBoltNamePrefix() + "Bolt";
            BaseRichBolt bolt = null;
            try {
                bolt = (BaseRichBolt) Class.forName("com.meituan.storm.bolt." + boltName).newInstance();
            } catch (Exception e) {
                throw new RuntimeException("Can not instantiate class com.meituan.storm.bolt." + boltName);
            }

            BoltDeclarer boltDeclarer = builder.setBolt(boltInfo.getComponentId(), bolt, boltInfo.getBoltExecutorNum());
            for (String dataSource : boltInfo.getDataSource())
                boltDeclarer.shuffleGrouping(dataSource);
        }
    }

    public TopologyBuilder createTopologyBuilder() {
        TopologyBuilder builder = new TopologyBuilder();

        // create spouts and filter bolts
        configSpoutsAndFilterBolts(builder);

        // create process bolt which get data from filter bolts and process them
        if (_boltInfos != null) {
            configProcessBolts(builder);
        }

        return builder;
    }

    public static void main(String[] args) throws Exception {
        int argNum = args.length;
        if (argNum > 3) {
            LOG.error("usage: storm jar FilterTopology-1.0.6-jar-with-dependencies.jar " +
                    "com.meituan.storm.topology.FilterTopology " +
                    "[src/main/resources/topology_config.json]");
            System.exit(-1);
        }

        FilterTopology filterTopology = null;
        if (argNum == 3)
            filterTopology = new FilterTopology(args[1]);
        else
            filterTopology = new FilterTopology();
        filterTopology.loadConfigurations();

        TopologyBuilder builder = filterTopology.createTopologyBuilder();
        Config conf = new Config();
        int workerNum = filterTopology.getWorkerNum();
        conf.setNumWorkers(workerNum);
        conf.setNumAckers(workerNum);
        conf.setDebug(filterTopology.getDebugMode());
        conf.registerMetricsConsumer(MTMetricsConsumer.class, 2);
        //StormSubmitter.submitTopology(filterTopology.getTopologyName(), conf, builder.createTopology());
        StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
    }
}
