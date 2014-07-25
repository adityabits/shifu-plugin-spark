import java.util.ArrayList;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;


public class StaticFunctions {

    public class Normalize implements Function<String, String> {
        /*
        DefaultTransformExecutor executor;
        Normalize(DefaultTransformExecutor executor) {
            this.executor= executor;
        }
        */

        Broadcast<PMML> bpmml;
        Broadcast<DefaultTransformExecutor> bexec;
        Broadcast<List<DataField>> dataFields;

        Normalize(Broadcast<PMML> bpmml, Broadcast<DefaultTransformExecutor> bexec, Broadcast<List<DataField>> bDataFields, Broadcast<List<Derived>> bActiveFields) {
            this.executor= executor;
            this.bpmml= bpmml;
            this.bexec= bexec;
            this.bDataFields= bDataFields;
            this.bActiveFields= bActiveFields;
        }

         
        @Override
        public String call(String input, String output) {
            List<Object> parsedInput= new ArrayList<Object>();
            
            // Put this step into shifu.core.utils
            for(String s: Splitter.on(delimiter).split(line)) {
                // Is this step required?
                parsedInput.add(s);
            }

            Map<String, Object> rawDataMap= new HashMap<String, Object>();
            for(int i=0; i < parsedInput.size(); i++) {
                rawDataMap.put(dataFields.get(i).getName().getValue(), parsedInput.get(i));
            }

            List<Object> result= executor.transform(targetFields, rawDataMap);
            result.addAll(executor.transform(activeFields, rawDataMap));
            return Joiner.on(",").join(result);
            
        }
    }
}
