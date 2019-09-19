package json.test.JSONTopo;

import java.awt.image.BufferedImage;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

import javax.imageio.ImageIO;

import org.apache.storm.multilang.JsonSerializer;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.json.JSONArray;
import org.json.JSONException;

public class InputSpout extends BaseRichSpout {

	SpoutOutputCollector _collector;
	static byte[] input;
	String jsonPath;
	File file;
	//String path;
	static int count;
	
	
	InputSpout(String jsonPath) {
		this.jsonPath = jsonPath;
				
	}
	

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this._collector = collector;
		file = new File(jsonPath);
		count = 0;
	}

	@Override
	public void nextTuple() {
		
		
		
					try {
						
						
						BufferedReader rd = null;

						rd = new BufferedReader(new FileReader(file));
						StringBuilder sb = new StringBuilder();
						String line = "";

						try {
							while ((line = rd.readLine()) != null) {
								line += ("\n");
								sb.append(line);
							}
						} catch (IOException e) {	
						}
						JSONArray JSON = new JSONArray(sb.toString());
						//input = readAllBytesOrExit(tFile.toPath());
						JsonSerializer serializer = new JsonSerializer();
						//serializer.writeSpoutMsg(JSON);
						this._collector.emit(new Values(JSON));
						//count++;
						
						//System.out.println("Count: " + count + " / byte[]: " + input);
						
						Thread.sleep(100);
						
					} catch (InterruptedException e) {
						
						e.printStackTrace();
						
					} catch (FileNotFoundException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					} catch (JSONException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					
				}
			
		
		
		
//		try {
//			Thread.sleep(100);
//		} catch (InterruptedException e) {
//			e.printStackTrace();
//		}
		
		

	@Override	
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("json"));
	}
	
	private static byte[] readAllBytesfromFile(File file) {
		return null;
		//return Files.readAllBytes(file);
		
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
	
	

	public static byte[] imagetoBytes(File imagePath) throws IOException {
		
		BufferedImage bImage;
		
		bImage = ImageIO.read(imagePath);
		
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		
		ImageIO.write(bImage, "jpg", bos);
		
		byte[] data = bos.toByteArray();
		return data;

	}

}
