package main.storm.DetectURLTopo;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.tensorflow.SavedModelBundle;
import org.tensorflow.Session;
import org.tensorflow.Tensor;

public class TensorBolt extends BaseRichBolt {
	
	OutputCollector collector;
	static int ROW = 0;
    static int FEATURE = 0;
	
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		String modelDir = "/home/team1/juhong/kepco/tensorflowforjava/resultmodel/PythonModel/result/regressionModel";
		String raw = (String) input.getValueByField("input");
		getDataSize(raw);
		
		float[][] testInput = new float[ROW][FEATURE];
		
		csvToMtrx(raw, testInput);
		
		try(SavedModelBundle b = SavedModelBundle.load(modelDir, "serve")){

            // create a session from the Bundle
            Session sess = b.session();
            
            // create an input Tensor
            Tensor x = Tensor.create(testInput);

            // run the model and get the result
            float[][] y = sess.runner()
                    .feed("x", x)
                    .fetch("h")
                    .run()
                    .get(0)
                    .copyTo(new float[ROW][1]);

            // print out the result
            for(int i=0; i<y.length; i++) {
                System.out.println(y[i][0]);
                System.out.println("Latency: " + (System.nanoTime() - input.getLongByField("start-time")));
            }
            	
        }
    }	
		
	

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
	}
	
	public static void getDataSize(String raw) {
		String[] field = null;
		field = raw.split(",");
		ROW = 1;
		FEATURE = field.length;
	}
	
	public static void csvToMtrx(String raw, float[][] mtrx) {
		String[] field = null;
		field = raw.split(",");
		for(int j=0; j<field.length; j++)
			mtrx[0][j] = Float.parseFloat(field[j]);
	}
	
	 public static void printMatrix(float[][] mtrx) {
	        System.out.println("============ARRAY VALUES============");
	        for(int i=0; i<mtrx.length; i++) {
	            if(i==0)
	                System.out.print("[");
	            else
	                System.out.println();
	            for(int j =0; j<mtrx[i].length; j++) {
	                System.out.print("["+mtrx[i][j]+"]");
	            }
	        }
	        System.out.println("]");
	    }
	
}
