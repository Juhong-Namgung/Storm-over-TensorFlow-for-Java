package test.perf.image.PerfTopo;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

public class LastBolt extends BaseRichBolt {
    private OutputCollector collector;
    private byte[] imageBytes;
	private long lead_time;
	private long rcv_number_of_tuples;
	private long total_size;

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.lead_time = 0L;
		this.rcv_number_of_tuples = 0L;
		this.total_size = 0L;		
    }

    public void execute(Tuple tuple) {
    	imageBytes = tuple.getBinaryByField("img");
		String label = (String) tuple.getValueByField("label");
		float prob = tuple.getFloatByField("probability");
		//this.lead_time += (System.currentTimeMillis() - tuple.getLongByField("start-time"));
		this.rcv_number_of_tuples += 1L;	
		this.total_size += tuple.getBinaryByField("img").length; 
		System.out.println("Imgae(byte): " + imageBytes + " 's Predict Result: " + label + " (" + prob + "%)");		
        this.collector.ack(tuple);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }
}