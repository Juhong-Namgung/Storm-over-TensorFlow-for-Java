package separate.inception.main.ImageTopology;

import org.tensorflow.Tensor;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class TensorSerializer extends Serializer<Tensor> {

	// https://www.baeldung.com/kryo
	
	
	@Override
	public void write(Kryo kryo, Output output, Tensor object) {
		// TODO Auto-generated method stub
		kryo.writeObject(output, object);
		//output.write(object);
		//output.writeO
		
	}

	@Override
	public Tensor read(Kryo kryo, Input input, Class<Tensor> type) {
		// TODO Auto-generated method stub
		return null;
	}

}
