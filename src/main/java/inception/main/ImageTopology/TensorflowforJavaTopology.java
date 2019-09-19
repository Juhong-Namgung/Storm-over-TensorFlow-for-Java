package inception.main.ImageTopology;

import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.KillOptions;
import org.apache.storm.generated.Nimbus.Client;
import org.apache.storm.generated.NotAliveException;
import org.apache.storm.kafka.BrokerHosts;
//import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StringScheme;

import org.apache.storm.kafka.ZkHosts;

import org.apache.storm.kafka.bolt.MyKafkaBolt;
import org.apache.storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper;
import org.apache.storm.kafka.bolt.selector.DefaultTopicSelector;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.thrift.TException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.NimbusClient;
import org.apache.storm.utils.Utils;

public class TensorflowforJavaTopology {

	private static Log LOG = LogFactory.getLog(TensorflowforJavaTopology.class);

	public static void main(String[] args)
			throws InterruptedException, NotAliveException, TException {
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
			int num_of_Bolts = Integer.parseInt(num_of_Node);
			
		
			zkhosts = "MN:42181,SN01:42181,SN02:42181,SN03:42181,SN04:42181,SN05:42181,SN06:42181,SN07:42181,SN08:42181";
			bootstrap = "MN:9092,SN01:9092,SN02:9092,SN03:9092,SN04:9092,SN05:9092,SN06:9092,SN07:9092,SN08:9092";
			
			
			/* Kafka Spout Configuration */
			BrokerHosts brokerHosts = new ZkHosts(
					zkhosts);

			SpoutConfig kafkaSpoutConfig = new SpoutConfig(brokerHosts, inputTopic, "/" + inputTopic,
					UUID.randomUUID().toString());
			kafkaSpoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
	
			/* KafkaBolt Configuration */
			
			Properties props = new Properties();
			props.put("metadata.broker.list",
					"MN:9092,SN01:9092,SN02:9092,SN03:9092,SN04:9092,SN05:9092,SN06:9092,SN07:9092,SN08:9092");
			props.put("bootstrap.servers", bootstrap);
			props.put("acks", "1");
			props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
			props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

			/* Kafka Spout */
			//KafkaSpout kafkaSpout = new KafkaSpout(kafkaSpoutConfig);			
			
			
			InputSpout inputSpout = new InputSpout("/home/team1/juhong/kepco/tensorflowforjava/data/");
			TensorBolt tensorBolt = new TensorBolt();		
			
			
			ReportBolt reportBolt = new ReportBolt();

			
			
			
			/* KafkaBolt */
			MyKafkaBolt kafkabolt = new MyKafkaBolt().withProducerProperties(props)
					.withTopicSelector(new DefaultTopicSelector(outputTopic))
					.withTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper());
			;

			// Topology Build
			TopologyBuilder builder = new TopologyBuilder();
			
			builder.setSpout("input-spout", inputSpout);
			builder.setBolt("tensor-bolt", tensorBolt, num_of_Bolts).shuffleGrouping("input-spout").setNumTasks(num_of_Bolts);
			builder.setBolt("report-bolt", reportBolt).shuffleGrouping("tensor-bolt");
			
			
			Config config = new Config();
			config.setNumWorkers(num_of_workers);

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
		}
		else {
			System.out.println("!!!!!!!!!!!!");
			System.out.println("Usage: $STORM_HOME/bin/storm jar [jar-file] [topologyName] [inputTopic] [outputTopic] [test_Time (min)] [num_of_Workers]");
		}
	} 
}