package test.perf.image.PerfTopo;

import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import javax.imageio.ImageIO;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class FirstSpout extends BaseRichSpout {
    
    private SpoutOutputCollector collector;
    private byte[] input;
	private String imagePath;
	private File file;
    private int interval;
	private int count = 0;
    

    public FirstSpout(String imagePath, int interval) {
    	this.imagePath = imagePath;
    	this.interval = interval;
    }
    
	public void nextTuple() {
		/*collector.emit(new Values(new byte[1024]), this.count++);
		 try {
	            Thread.sleep(50);
	        } catch (InterruptedException e) {
	            e.printStackTrace();
	        }*/
		
		//collector.emit(new Values(new byte[size]), this.count++);
		file = new File(imagePath);

		if (file.isDirectory()) {
			File[] fileList = file.listFiles();
			for (File tFile : fileList) {
				if (!tFile.isDirectory()) {
					input = readAllBytesOrExit(tFile.toPath());
					collector.emit(new Values(input), this.count++);
				}

				try {
					Thread.sleep(interval);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
		
	}

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("input"));
    }

    public void ack(Object msgId) {
        super.ack(msgId);
    }

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		// TODO Auto-generated method stub
		   this.collector = collector;
	}
	
	public byte[] readAllBytesOrExit(Path path) {
		try {
			return Files.readAllBytes(path);
			
		} catch (IOException e) {
			System.err.println(e.toString());
			System.err.println("Failed to read [" + path + "]: " + e.getMessage());
			System.err.println("Here");
			System.exit(1);
		}
		return null;
	}
	
	public byte[] imagetoBytes(File imagePath) throws IOException {
		
		BufferedImage bImage;
		
		bImage = ImageIO.read(imagePath);
		
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		
		ImageIO.write(bImage, "jpg", bos);
		
		byte[] data = bos.toByteArray();
		return data;

	}
}
