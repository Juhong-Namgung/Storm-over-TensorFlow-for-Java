package separate.inception.main.ImageTopology;

import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.KillOptions;
import org.apache.storm.generated.Nimbus.Client;
import org.apache.storm.generated.NotAliveException;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.BrokerHosts;
//import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StringScheme;

import org.apache.storm.kafka.ZkHosts;

import org.apache.storm.kafka.bolt.MyKafkaBolt;
import org.apache.storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper;
import org.apache.storm.kafka.bolt.selector.DefaultTopicSelector;
import org.apache.storm.perf.utils.Helper;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.thrift.TException;
import org.apache.storm.topology.BoltDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.NimbusClient;
import org.apache.storm.utils.Utils;

public class MetricsTFJavaTopo {

	private static Log LOG = LogFactory.getLog(MetricsTFJavaTopo.class);

	
	private static final String TOPOLOGY_NAME = "MetrcisTensorFlowTopo";

	private static final String FIRST_SPOUT_ID = "InputSpout";
	private static final String PREPROCESSING_BOLT_ID = "PreProcessingBolt";
	private static final String TENSOR_BOLT_ID = "TensorBolt";
	private static final String REPORT_BOLT_ID = "ReportBolt";
	
	

	private static StormTopology getTopology(Map conf) {
		TopologyBuilder builder = new TopologyBuilder();

		// 1 - Setup Spout --------

		InputSpout spout = new InputSpout("/home/team1/juhong/kepco/tensorflowforjava/sampledata/");
		

		// 2 - Setup Bolts --------
		
		PreProcessingBolt preprocessingBolt = new PreProcessingBolt();
		TensorBolt tensorBolt = new TensorBolt();
		ReportBolt reportBolt = new ReportBolt();
		
		// 3 - Setup Topology
		
		builder.setSpout(FIRST_SPOUT_ID, spout, Helper.getInt(conf, "first.spout.executors.count", 1))
				.setNumTasks(Helper.getInt(conf, "first.spout.tasks.count", 1));
		//String prevTaskID = FIRST_SPOUT_ID;
		
		builder.setBolt(PREPROCESSING_BOLT_ID, preprocessingBolt, Helper.getInt(conf, "prepro.bolt.executors.count",1))
		.setNumTasks(Helper.getInt(conf, "prepro.bolt.tasks.count", 1)).shuffleGrouping(FIRST_SPOUT_ID);
		
		builder.setBolt(TENSOR_BOLT_ID, tensorBolt, Helper.getInt(conf, "tensor.bolt.executors.count",1))
		.setNumTasks(Helper.getInt(conf, "tensor.bolt.tasks.count", 1)).shuffleGrouping(PREPROCESSING_BOLT_ID);

		builder.setBolt(REPORT_BOLT_ID, reportBolt, Helper.getInt(conf, "report.bolt.executors.count",1))
		.setNumTasks(Helper.getInt(conf, "report.bolt.tasks.count", 1)).shuffleGrouping(TENSOR_BOLT_ID);
		
		return builder.createTopology();
		
	}
				
		/*
		builder.setBolt(BOLT1_ID, bolt1, Helper.getInt(conf, BOLT1_COUNT, 1))
        .localOrShuffleGrouping(SPOUT_ID);
		
		for (int i = 1; i <= Helper.getInt(conf, "middle.bolt.count", 0); i++) {
			BoltDeclarer middleBolt = builder
					.setBolt(MIDDLE_BOLT_ID + i, new MiddleBolt(),
							Helper.getInt(conf, "middle.bolt.executors.count", 1))
					.setNumTasks(Helper.getInt(conf, "middle.bolt.tasks.count", 1));
			setGrouping(middleBolt, conf, prevTaskID);
			prevTaskID = MIDDLE_BOLT_ID + i;
		}

		// 3 - Setup Last Bolt --------
		BoltDeclarer lastBolt = builder
				.setBolt(LAST_BOLT_ID, new LastBolt(), Helper.getInt(conf, "last.bolt.executors.count", 1))
				.setNumTasks(Helper.getInt(conf, "last.bolt.tasks.count", 1));
		setGrouping(lastBolt, conf, prevTaskID);

		return builder.createTopology();
	}
	
	*/
	
	
	public static void main(String[] args)
			throws Exception {

		if (args.length <= 0) {
	            // submit to local cluster
	            Config conf = new Config();
	            LocalCluster cluster = Helper.runOnLocalCluster(TOPOLOGY_NAME, getTopology(conf));

	            Helper.setupShutdownHook(cluster, TOPOLOGY_NAME);
	            while (true) {//  run indefinitely till Ctrl-C
	                Thread.sleep(20_000_000);
	            }
	        } else {
	            // submit to real cluster
	            if (args.length >2) {
	                System.err.println("args: runDurationSec  [optionalConfFile]");
	                return;
	            }
	            Integer durationSec = Integer.parseInt(args[0]);
	            Map topoConf =  (args.length==2) ? Utils.findAndReadConfigFile(args[1])  : new Config();
	            
	            topoConf.put(Config.TOPOLOGY_DISABLE_LOADAWARE_MESSAGING, true);


	            
	            //StormSubmitter.submitTopology(TOPOLOGY_NAME, topoConf,  getTopology(topoConf));
	            //  Submit topology to storm cluster
	            Helper.runOnClusterAndPrintMetrics(durationSec, TOPOLOGY_NAME, topoConf, getTopology(topoConf));
	        }
	    }
		
		
		
		
		
		
		
		
		
		
		/*
		if (args.length > 5) {
			String topologyName = args[0];
			String inputTopic = args[1];
			String outputTopic = args[2];
			String num_of_Node = args[3];
			String testTime = args[4];
			String num_of_Workers = args[5];
			int test_Time = Integer.parseInt(testTime);
			String zkhosts = null;
			String bootstrap = null;
			int num_of_workers = Integer.parseInt(num_of_Workers);
			
			
		
			zkhosts = "MN:42181,SN01:42181,SN02:42181,SN03:42181,SN04:42181,SN05:42181,SN06:42181,SN07:42181,SN08:42181";
			bootstrap = "MN:9092,SN01:9092,SN02:9092,SN03:9092,SN04:9092,SN05:9092,SN06:9092,SN07:9092,SN08:9092";
			
			
			// Kafka Spout Configuration 
			BrokerHosts brokerHosts = new ZkHosts(
					zkhosts);

			SpoutConfig kafkaSpoutConfig = new SpoutConfig(brokerHosts, inputTopic, "/" + inputTopic,
					UUID.randomUUID().toString());
			kafkaSpoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
	
			// KafkaBolt Configuration 
			
			Properties props = new Properties();
			props.put("metadata.broker.list",
					"MN:9092,SN01:9092,SN02:9092,SN03:9092,SN04:9092,SN05:9092,SN06:9092,SN07:9092,SN08:9092");
			props.put("bootstrap.servers", bootstrap);
			props.put("acks", "1");
			props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
			props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

			// Kafka Spout
			//KafkaSpout kafkaSpout = new KafkaSpout(kafkaSpoutConfig);			
			
			
			InputSpout inputSpout = new InputSpout("/home/team1/juhong/kepco/tensorflowforjava/data/");
			
			PreProcessingBolt preprocessingBolt = new PreProcessingBolt();
			TensorBolt tensorBolt = new TensorBolt();		
			
			
			ReportBolt reportBolt = new ReportBolt();	

			
			
			
			// KafkaBolt
			MyKafkaBolt kafkabolt = new MyKafkaBolt().withProducerProperties(props)
					.withTopicSelector(new DefaultTopicSelector(outputTopic))
					.withTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper());
			;

			// Topology Build
			TopologyBuilder builder = new TopologyBuilder();
			
			builder.setSpout("input-spout", inputSpout);
			builder.setBolt("preprocessing-bolt", preprocessingBolt, 2).shuffleGrouping("input-spout").setNumTasks(2);
			builder.setBolt("tensor-bolt", tensorBolt, 4).shuffleGrouping("preprocessing-bolt").setNumTasks(4);
			builder.setBolt("report-bolt", reportBolt).shuffleGrouping("tensor-bolt");
			
			
			Config config = new Config();
			config.setNumWorkers(num_of_workers);
			
			try {
				Helper.runOnClusterAndPrintMetrics(200000, topologyName, config, builder.createTopology());
				
				
			} catch (Exception e) {
				e.printStackTrace();
			}

			*/


			/*
			StormSubmitter.submitTopology(topologyName, config, builder.createTopology());
		
			try {
				Thread.sleep(test_Time * 60 * 1000);

				Map<String, Object> conf = Utils.readStormConfig();
				Client client = NimbusClient.getConfiguredClient(conf).getClient();
				KillOptions killOpts = new KillOptions();
				//killOpts.set_wait_secs(0);
				client.killTopologyWithOpts(topologyName, killOpts);
			} catch(AlreadyAliveException ae){
				LOG.info(ae.get_msg());
			}catch(InvalidTopologyException ie){
				LOG.info(ie.get_msg());
			}
		} */
		/*}
		else {
			System.out.println("!!!!!!!!!!!!");
			System.out.println("Usage: $STORM_HOME/bin/storm jar [jar-file] [topologyName] [inputTopic] [outputTopic] [test_Time (min)] [num_of_Workers]");
		}
		*/
	
}