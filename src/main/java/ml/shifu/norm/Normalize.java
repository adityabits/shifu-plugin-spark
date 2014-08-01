package ml.shifu.norm;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import ml.shifu.core.di.builtin.transform.DefaultTransformationExecutor;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import org.dmg.pmml.DataField;
import org.dmg.pmml.DerivedField;
import org.dmg.pmml.PMML;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;

public class Normalize implements Function<String, String> {
		/*
        private Broadcast<PMML> bpmml;
        private Broadcast<DefaultTransformationExecutor> bexec;
		private Broadcast<List<DataField>> bDataFields;
		private Broadcast<List<DerivedField>> bActiveFields;
		private Broadcast<List<DerivedField>> bTargetFields;
		
        Normalize(Broadcast<PMML> bpmml, Broadcast<DefaultTransformationExecutor> bexec, Broadcast<List<DataField>> bDataFields, Broadcast<List<DerivedField>> bActiveFields, Broadcast<List<DerivedField>> bTargetFields) {
            this.bpmml= bpmml;
            this.bexec= bexec;
            this.bDataFields= bDataFields;
            this.bActiveFields= bActiveFields;
            this.bTargetFields= bTargetFields;
        }
		*/
		private BroadcastVariables bVar;
		private String delimiter= ",";
 
        public Normalize(BroadcastVariables bVar) {
			// TODO Auto-generated constructor stub
        	this.bVar= bVar;
		}


		@Override
        public String call(String input) {
            List<Object> parsedInput= new ArrayList<Object>();
            
            // Put this step into shifu.core.utils
            for(String s: Splitter.on(delimiter).split(input)) {
                parsedInput.add(s);
            }

            Map<String, Object> rawDataMap= new HashMap<String, Object>();
            for(int i=0; i < parsedInput.size(); i++) {
                rawDataMap.put(bVar.getDataFields().get(i).getName().getValue(), parsedInput.get(i));
            }

            List<Object> result= bVar.getExec().transform(bVar.getTargetFields(), rawDataMap);
            result.addAll(bVar.getExec().transform(bVar.getActiveFields(), rawDataMap));
            return Joiner.on(delimiter).join(result);
            
        }
    }

