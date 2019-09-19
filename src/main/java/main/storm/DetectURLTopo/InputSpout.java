package main.storm.DetectURLTopo;

import java.util.Map;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class InputSpout extends BaseRichSpout {

	SpoutOutputCollector _collector;
	static String input;
	

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this._collector = collector;
		input = "";
	}

	@Override
	public void nextTuple() {
		
		input = makeRandomCSV();		
		this._collector.emit(new Values(input, System.nanoTime()));
		try {
			Thread.sleep(100);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		System.out.println(input);
	}

	@Override	
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("input", "start-time"));
	}
	
	public static String makeRandomCSV() {
		double randomValue; 
		int intValue = 0;
		String output = "";
		
		randomValue = Math.random();
		intValue= (int)(randomValue * 2);
		
		output = output + intValue ;
		for(int i=1; i<4; i++) {
			randomValue = Math.random();
			intValue= (int)(randomValue * 2);
			//System.out.println(intValue);
			output = output + "," + intValue;
		}		
		return output;
	}

}
