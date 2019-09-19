package separate.inception.main.ImageTopology;

import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

import javax.imageio.ImageIO;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class InputSpout extends BaseRichSpout {

	private SpoutOutputCollector collector;
	private byte[] input;
	private String imagePath;
	File file;
	//String path;
	private int count = 0;
	
	
	InputSpout(String imagePath) {
		this.imagePath = imagePath;
				
	}
	

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
		file = new File(imagePath);		
	}

	@Override
	public void nextTuple() {
		
		
		if (file.isDirectory()) {
			File[] fileList = file.listFiles();
			for (File tFile : fileList) {
				if(!tFile.isDirectory()) {
					//input = readAllBytesOrExit(Paths.get(tFile));
					try {
						
						input = readAllBytesOrExit(tFile.toPath());
						collector.emit(new Values(input, System.currentTimeMillis()));
						
						
						count++;
						
						//System.out.println("Count: " + count + " / byte[]: " + input);
						
						Thread.sleep(100);
						
					} catch (InterruptedException e) {
						
						e.printStackTrace();
						
					}
					
				}
			}
		}
				
		
		
		
//		try {
//			Thread.sleep(100);
//		} catch (InterruptedException e) {
//			e.printStackTrace();
//		}
		
		//System.out.println("Count: " + count + " / byte[]: " + input);
	}

	@Override	
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("input", "start-time"));
	}	
	
	private static byte[] readAllBytesOrExit(Path path) {
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
	
	
	public void ack(Object msgId) {
		super.ack(msgId);
	}

	public static byte[] imagetoBytes(File imagePath) throws IOException {
		
		BufferedImage bImage;
		
		bImage = ImageIO.read(imagePath);
		
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		
		ImageIO.write(bImage, "jpg", bos);
		
		byte[] data = bos.toByteArray();
		return data;

	}

}
