package perf.inception.main.ImageTopology;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.tensorflow.DataType;
import org.tensorflow.Graph;
import org.tensorflow.Output;
import org.tensorflow.SavedModelBundle;
import org.tensorflow.Session;
import org.tensorflow.Tensor;
import org.tensorflow.types.UInt8;

public class TensorBolt extends BaseRichBolt {

	OutputCollector collector;
	static int ROW = 0;
	static int FEATURE = 0;
	byte[] graphDefs;
	String modelDir = "/home/team1/juhong/kepco/tensorflowforjava/resultmodel/inception5h";
	String modelName = "tensorflow_inception_graph.pb";
	List<String> labels;
	private byte[] imageBytes;

	static long count;

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
	
		this.collector = collector;
		this.graphDefs = readAllBytesOrExit(Paths.get(modelDir, modelName));
		this.labels = readAllLinesOrExit(Paths.get(modelDir, "imagenet_comp_graph_label_strings.txt"));	
	
		this.count = 0L;

	}

	@Override
	public void execute(Tuple input) {

		// From Spout...
		//String imageFile = "";
		//byte[] imageBytes = readAllBytesOrExit(Paths.get(imageFile));
		imageBytes = input.getBinaryByField("input");
		long start = System.currentTimeMillis();
		System.out.println("=====================");
		System.out.println("Image Graph Execution");
		try (Tensor<Float> image = constructAndExecuteGraphToNormalizeImage(imageBytes)) {
		
			System.out.println("======================");
			System.out.println(System.currentTimeMillis() - start);
			System.out.println("======================");
			
			start = System.currentTimeMillis();
			System.out.println("=====================");
			System.out.println("Inception Graph Execution");
			
			float[] labelProbabilities = executeInceptionGraph(graphDefs, image);
			
			System.out.println("======================");
			System.out.println(System.currentTimeMillis() - start);
			System.out.println("======================");
			
			
			start = System.currentTimeMillis();
			System.out.println("=====================");
			System.out.println("Find a max index");
			int bestLabelIdx = maxIndex(labelProbabilities);
			//latency = (System.currentTimeMillis() - input.getLongByField("start-time"));
			//total_latency += latency;
			count += 1L;
			System.out.println(String.format("BEST MATCH: %s (%.2f%% likely)", labels.get(bestLabelIdx),
					labelProbabilities[bestLabelIdx] * 100f));			
			//count ++;
			
			System.out.println("======================");
			System.out.println(System.currentTimeMillis() - start);
			System.out.println("======================");
			
			start = System.currentTimeMillis();
			System.out.println("=====================");
			System.out.println("Emit the tuple");
			collector.emit(new Values(imageBytes, labels.get(bestLabelIdx), labelProbabilities[bestLabelIdx] * 100f, input.getLongByField("start-time")));
			
			System.out.println("======================");
			System.out.println(System.currentTimeMillis() - start);
			System.out.println("======================");
			
		}

	}
	
	@Override	
	public void cleanup() {
		System.out.println("Total msg: " + count);
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("img", "label", "probability", "start-time"));
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

	private static List<String> readAllLinesOrExit(Path path) {
		try {
			return Files.readAllLines(path, Charset.forName("UTF-8"));
		} catch (IOException e) {
			System.err.println("Failed to read [" + path + "]: " + e.getMessage());
			System.exit(0);
		}
		return null;
	}

	private static Tensor<Float> constructAndExecuteGraphToNormalizeImage(byte[] imageBytes) {
		try (Graph g = new Graph()) {
			GraphBuilder b = new GraphBuilder(g);
			// Some constants specific to the pre-trained model at:
			// https://storage.googleapis.com/download.tensorflow.org/models/inception5h.zip
			//
			// - The model was trained with images scaled to 224x224 pixels.
			// - The colors, represented as R, G, B in 1-byte each were converted to
			// float using (value - Mean)/Scale.
			final int H = 224;
			final int W = 224;
			final float mean = 117f;
			final float scale = 1f;

			// Since the graph is being constructed once per execution here, we can use a
			// constant for the
			// input image. If the graph were to be re-used for multiple input images, a
			// placeholder would
			// have been more appropriate.
			final Output<String> input = b.constant("input", imageBytes);
			final Output<Float> output = b
					.div(b.sub(
							b.resizeBilinear(b.expandDims(b.cast(b.decodeJpeg(input, 3), Float.class),
									b.constant("make_batch", 0)), b.constant("size", new int[] { H, W })),
							b.constant("mean", mean)), b.constant("scale", scale));
			try (Session s = new Session(g)) {
				// Generally, there may be multiple output tensors, all of them must be closed
				// to prevent resource leaks.
				return s.runner().fetch(output.op().name()).run().get(0).expect(Float.class);
			}
		}
	}

	private static float[] executeInceptionGraph(byte[] graphDef, Tensor<Float> image) {
		try (Graph g = new Graph()) {
			g.importGraphDef(graphDef);
			Tensor<Float> mid_result_out = null;

			try (Session s = new Session(g);
					// Generally, there may be multiple output tensors, all of them must be closed
					// to prevent resource leaks.
					Tensor<Float> result = s.runner().feed("input", image).fetch("output").run().get(0)
							.expect(Float.class)) {
				//System.out.println(image);
				//System.out.println(result);
				final long[] rshape = result.shape();
				if (result.numDimensions() != 2 || rshape[0] != 1) {
					throw new RuntimeException(String.format(
							"Expected model to produce a [1 N] shaped tensor where N is the number of labels, instead it produced one with shape %s",
							Arrays.toString(rshape)));
				}
				int nlabels = (int) rshape[1];
				return result.copyTo(new float[1][nlabels])[0];
			}
		}
	}

	private static int maxIndex(float[] probabilities) {
		int best = 0;
		for (int i = 1; i < probabilities.length; ++i) {
			if (probabilities[i] > probabilities[best]) {
				best = i;
			}
		}
		return best;
	}

	static class GraphBuilder {
		GraphBuilder(Graph g) {
			this.g = g;
		}

		Output<Float> div(Output<Float> x, Output<Float> y) {
			return binaryOp("Div", x, y);
		}

		<T> Output<T> sub(Output<T> x, Output<T> y) {
			return binaryOp("Sub", x, y);
		}

		<T> Output<Float> resizeBilinear(Output<T> images, Output<Integer> size) {
			return binaryOp3("ResizeBilinear", images, size);
		}

		<T> Output<T> expandDims(Output<T> input, Output<Integer> dim) {
			return binaryOp3("ExpandDims", input, dim);
		}

		<T, U> Output<U> cast(Output<T> value, Class<U> type) {
			DataType dtype = DataType.fromClass(type);
			return g.opBuilder("Cast", "Cast").addInput(value).setAttr("DstT", dtype).build().<U>output(0);
		}

		Output<UInt8> decodeJpeg(Output<String> contents, long channels) {
			return g.opBuilder("DecodeJpeg", "DecodeJpeg").addInput(contents).setAttr("channels", channels).build()
					.<UInt8>output(0);
		}

		<T> Output<T> constant(String name, Object value, Class<T> type) {
			try (Tensor<T> t = Tensor.<T>create(value, type)) {
				return g.opBuilder("Const", name).setAttr("dtype", DataType.fromClass(type)).setAttr("value", t).build()
						.<T>output(0);
			}
		}

		Output<String> constant(String name, byte[] value) {
			return this.constant(name, value, String.class);
		}

		Output<Integer> constant(String name, int value) {
			return this.constant(name, value, Integer.class);
		}

		Output<Integer> constant(String name, int[] value) {
			return this.constant(name, value, Integer.class);
		}

		Output<Float> constant(String name, float value) {
			return this.constant(name, value, Float.class);
		}

		private <T> Output<T> binaryOp(String type, Output<T> in1, Output<T> in2) {
			return g.opBuilder(type, type).addInput(in1).addInput(in2).build().<T>output(0);
		}

		private <T, U, V> Output<T> binaryOp3(String type, Output<U> in1, Output<V> in2) {
			return g.opBuilder(type, type).addInput(in1).addInput(in2).build().<T>output(0);
		}

		private Graph g;
	}

}
