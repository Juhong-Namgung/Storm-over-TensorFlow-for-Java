package test.perf.main.PerfTopo;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.generated.TopologySummary;
import org.apache.storm.perf.utils.Helper;
import org.apache.storm.topology.BoltDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;

import java.util.List;
import java.util.Map;

public class PerfTopo {

    private static final String TOPOLOGY_NAME = "LocalityPerformanceComparisonTopo";
    private static final String FIRST_SPOUT_ID = "FirstSpout";
    private static final String MIDDLE_BOLT_ID = "MiddleBolt";
    private static final String LAST_BOLT_ID = "LastBolt";

    private static StormTopology getTopology(Map conf) {
        TopologyBuilder builder = new TopologyBuilder();

        // 1 -  Setup Spout   --------
        FirstSpout spout = new FirstSpout(Helper.getInt(conf, "message.size", 1024),
                Helper.getInt(conf, "message.interval", 50));
        builder.setSpout(FIRST_SPOUT_ID, spout,  Helper.getInt(conf, "first.spout.executors.count", 1))
            .setNumTasks(Helper.getInt(conf, "first.spout.tasks.count", 1));
        String prevTaskID = FIRST_SPOUT_ID;

        // 2 -  Setup Middle Bolts   --------
        for (int i=1; i<=Helper.getInt(conf, "middle.bolt.count", 0); i++) {
            BoltDeclarer middleBolt = builder.setBolt(MIDDLE_BOLT_ID+i, new MiddleBolt(), Helper.getInt(conf, "middle.bolt.executors.count", 1))
                    .setNumTasks(Helper.getInt(conf, "middle.bolt.tasks.count", 1));
          
            prevTaskID = MIDDLE_BOLT_ID+i;
        }

        // 3 -  Setup Last Bolt   --------
        BoltDeclarer lastBolt = builder.setBolt(LAST_BOLT_ID, new LastBolt(), Helper.getInt(conf, "last.bolt.executors.count", 1))
                .setNumTasks(Helper.getInt(conf, "last.bolt.tasks.count", 1));
     
        return builder.createTopology();
    }



    public static void main(String[] args) throws Exception {
        // submit to local cluster
        if (args.length <= 0) {
            Config conf = new Config();
            LocalCluster cluster = Helper.runOnLocalCluster(TOPOLOGY_NAME, getTopology(conf));

            String topologyId = null;
            for (TopologySummary t : cluster.getClusterInfo().get_topologies()) {
                if (t.get_name().equals(TOPOLOGY_NAME))
                    topologyId = t.get_id();
            }

            Helper.collectLocalMetricsAndKill(cluster, topologyId, 10, 180, conf);
            while (true) {//  run indefinitely till Ctrl-C
                Thread.sleep(20_000_000);
            }
        }
        // submit to real cluster
        if (args.length >2) {
            System.err.println("args: runDurationSec  [optionalConfFile]");
        } else {
            Integer durationSec = Integer.parseInt(args[0]);
            Map<String, Object> topoConf = (args.length == 2) ? Utils.findAndReadConfigFile(args[1]) : new Config();
            topoConf.put(Config.TOPOLOGY_DISABLE_LOADAWARE_MESSAGING, true);

            //  Submit topology to storm cluster
            int countJob = Helper.getInt(topoConf, "topology.count", 1);
            while (countJob-- > 1)
                StormSubmitter.submitTopology(TOPOLOGY_NAME+"-"+countJob, topoConf, getTopology(topoConf));
            Helper.runOnClusterAndPrintMetrics(durationSec, TOPOLOGY_NAME+"-"+countJob, topoConf, getTopology(topoConf));

        }
    }
}