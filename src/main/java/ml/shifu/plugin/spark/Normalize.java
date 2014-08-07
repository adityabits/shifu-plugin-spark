/*
 * This is the user defined function which is called by Spark "map" on each record in the JavaRDD.
 * This map should be applied on the raw JavaRDD containing strings of raw rows from the input data file.
 * This class is initialized with an BroadcastVariables object as the single broadcast variable.
 * Normalization takes a single row of data and applies transformations defined in the PMML object by 
 * calling the transform() function of the TransformationExecutor object.
 * The resulting normalized row is again converted to a single string which is output.
 */

package ml.shifu.plugin.spark;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;

import com.google.common.base.Joiner;

public class Normalize implements Function<String, String> {
	//private BroadcastVariables bVar;
	private String delimiter= ",";
	// TODO: get this from bVar
	private Broadcast<BroadcastVariables> broadVar;
	private String pattern;
 
        public Normalize(Broadcast<BroadcastVariables> bVar) {
    	//this.bVar= bVar.value();
    	this.broadVar= bVar;
    	pattern= "%." + bVar.value().getPrecision() + "f";
	}


	@Override
    public String call(String input) {
		
		List<Object> parsedInput= CombinedUtils.getParsedObjects(input, delimiter);
        Map<String, Object> rawDataMap= CombinedUtils.createDataMap(broadVar.value().getDataFields(), parsedInput);
        List<Object> result= broadVar.value().getExec().transform(broadVar.value().getTargetFields(), rawDataMap);
        result.addAll(broadVar.value().getExec().transform(broadVar.value().getActiveFields(), rawDataMap));
        List<String> resultStr= new ArrayList<String>();
        for(Object r: result) {
        	if(r instanceof Float || r instanceof Double) {
        		System.out.println("original- " + r);
        		System.out.println("pattern= " + pattern);
        		resultStr.add(String.format(pattern,  r));
        		System.out.println("new str- " + String.format(pattern, r));
        		//resultStr.add(r.toString());
        	}
        	else
        		resultStr.add(r.toString());
        }
        
        return Joiner.on(delimiter).join(resultStr);
    }
}

