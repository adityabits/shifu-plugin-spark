package ml.shifu.norm;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

/*
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.scheduler.StreamingListener;
import org.apache.spark.streaming.scheduler.StreamingListenerBatchCompleted;
import org.apache.spark.streaming.api.java.JavaDStream;
*/

import com.google.common.base.Joiner;
import com.google.inject.Guice;
import com.google.inject.Injector;
import ml.shifu.core.di.module.SimpleModule;
import ml.shifu.core.di.service.TransformationExecService;
import ml.shifu.core.di.spi.SingleThreadFileLoader;
import ml.shifu.core.di.spi.RequestProcessor;
import ml.shifu.core.request.Request;
import ml.shifu.core.util.CSVWithHeaderLocalSingleThreadFileLoader;
import ml.shifu.core.util.PMMLUtils;
import org.dmg.pmml.DerivedField;
import org.dmg.pmml.FieldName;
import org.dmg.pmml.Model;
import org.dmg.pmml.PMML;

import java.io.PrintWriter;
import java.util.List;
import java.util.Map;

import ml.shifu.norm.StaticFunctions.Normalize;



public class SparkModelTransformRequestProcessor implements RequestProcessor {
    
    public void exec(Request req) throws Exception {

        Params params= req.getProcessor().getParams();

        String pathPMML = (String) params.get("pathPMML", "model.xml");

        PMML pmml = PMMLUtils.loadPMML(pathPMML);

        String pathOutputActiveHeader= params.get("pathOutputActiveHeader").toString();
        String pathInputData= params.get("pathInputData").toString();
        String pathOutputData= params.get("pathOutputData").toString();
        Model model= PMMLUtils.getModelByName(pmml, params.get("modelName").toString());
        Map<FieldUsageType, List<DerivedField>> fieldMap= PMMLUtils.getDerievdFieldsByUsageType(pmml, model);
        List<DerivedField> activeFields= fieldMap.get(FieldUsageType.ACTIVE);
        List<DerivedField> targetFields= fieldMap.get(FieldUsageType.TARGET);


        SparkConf conf= new SparkConf().setAppName("spark-norm").setMaster("local");
        //JavaStreamingContext jsc= new JavaStreamingContext(conf, new Duration(1000));
        //JavaDStream<String> lines= jsc.textFileStream(pathInputData);

        JavaSparkContext jsc= new JavaSparkContext(conf);
        JavaRDD<String> raw= jsc.textFile(pathInputData);
        
        DefaultTransformExecutor executor= new DefaultTransformExecutor();
        Broadcast<DefaultTransformExecutor> bexec= jsc.broadcast(executor);
        Broadcast<PMML> bpmml= jsc.broadcast(pmml);
        Broadcast<List<DataField>> bDataFields= jsc.broadcast(pmml.getDataDictionary().getDataFields());
        Broadcast<List<DerivedField>> bActiveFields= jsc.broadcast(activeFields);

        JavaRDD<String> normalized= raw.map(new Normalize(bexec, bpmml, bDataFields, bActiveFields));

        





    }
}
