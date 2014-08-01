package ml.shifu.norm;

import org.apache.spark.serializer.KryoRegistrator;
import org.dmg.pmml.DataField;
import org.dmg.pmml.DerivedField;
import org.dmg.pmml.PMML;

import com.esotericsoftware.kryo.Kryo;

public class MyRegistrator implements KryoRegistrator {

	public MyRegistrator() {
		// TODO Auto-generated constructor stub
	}

	@Override
	public void registerClasses(Kryo kryo) {
		// TODO Auto-generated method stub
		kryo.register(DataField.class);
		kryo.register(PMML.class);
		kryo.register(DerivedField.class);
	}

}
