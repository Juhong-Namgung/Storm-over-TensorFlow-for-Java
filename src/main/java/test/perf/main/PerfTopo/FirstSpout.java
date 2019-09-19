package test.perf.main.PerfTopo;

import java.util.Map;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class FirstSpout extends BaseRichSpout {
    private int size;
    private int interval;
    private SpoutOutputCollector collector;

    private int count = 0;

    public FirstSpout(int size, int interval) {
        this.size = size;
        this.interval = interval;
    }
    
    public void nextTuple() {
        collector.emit(new Values(new byte[size]), this.count++);
        try {
            Thread.sleep(interval);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("say"));
    }

    public void ack(Object msgId) {
        super.ack(msgId);
    }

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		// TODO Auto-generated method stub
		   this.collector = collector;
	}
}
