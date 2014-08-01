package ml.shifu.norm;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.transform.sax.SAXSource;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.dmg.pmml.DataField;
import org.dmg.pmml.PMML;
import org.jpmml.model.ImportFilter;
import org.jpmml.model.JAXBUtil;
import org.xml.sax.InputSource;

import com.google.common.base.Splitter;

public class CombinedUtils {

	public CombinedUtils() {
		// TODO Auto-generated constructor stub
	}
	
	// Should go into PMMLUtils
	public static PMML loadPMML(String pathPMML, FileSystem fs) {
		// load PMML from any fs- local or hdfs
		InputStream pmmlInputStream= null;
		PMML pmml= null;
		try {
		    pmmlInputStream = fs.open(new Path(pathPMML));
		    InputSource source = new InputSource(pmmlInputStream);
		    SAXSource transformedSource = ImportFilter.apply(source);
		    pmml=  JAXBUtil.unmarshalPMML(transformedSource);
		} catch (Exception e) {
		    e.printStackTrace();
		}
		return pmml;
	}

	public static Map<String, Object> createDataMap(List<DataField> dataFields,
			List<Object> parsedInput) {
        Map<String, Object> rawDataMap= new HashMap<String, Object>();
        for(int i=0; i < parsedInput.size(); i++) {
            rawDataMap.put(dataFields.get(i).getName().getValue(), parsedInput.get(i));
        }
		return null;
	}

	public static List<Object> getParsedObjects(String input, String delimiter) {
        List<Object> parsedInput= new ArrayList<Object>();
        
        // Put this step into shifu.core.utils
        for(String s: Splitter.on(delimiter).split(input)) {
            parsedInput.add(s);
        }
        return parsedInput;
	}

}
