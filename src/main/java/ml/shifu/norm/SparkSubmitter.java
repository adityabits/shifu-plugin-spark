package ml.shifu.norm;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.dmg.pmml.*;
import org.jpmml.evaluator.ExpressionUtil;
import org.jpmml.model.ImportFilter;
import org.jpmml.model.JAXBUtil;
import org.xml.sax.InputSource;

import ml.shifu.core.di.builtin.transform.DefaultTransformationExecutor;
import ml.shifu.core.di.module.SimpleModule;
import ml.shifu.core.di.service.TransformationExecService;
import ml.shifu.core.di.spi.RequestProcessor;
import ml.shifu.core.di.spi.TransformationExecutor;
import ml.shifu.core.request.Request;
import ml.shifu.core.util.PMMLUtils;
import ml.shifu.core.util.Params;
import ml.shifu.core.util.JSONUtils;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

import javax.xml.transform.sax.SAXSource;

public class SparkSubmitter {
    public static void main(String[] args) throws Exception
    {
        // argument 1: HDFS path to request.json
        // argument 2: HDFS path to PMML model.xml
        String pathReq= args[0];
        String pathPMML= args[1];

        FileSystem fs= FileSystem.get(new Configuration());

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

        //PMML pmml= CombinedUtils.loadPMML(pathPMML, fs);
        

        SparkModelTransformRequestProcessor strp= new SparkModelTransformRequestProcessor();
        Request req=  JSONUtils.readValue(fs.open(new Path(pathReq)), Request.class); 
        
        Params params= req.getProcessor().getParams();

        String pathInputData= params.get("pathInputData").toString();
        String pathOutputData= params.get("pathOutputData").toString();
        String pathHDFSTmp= (String) params.get("pathHDFSTmp", "ml/shifu/norm/tmp");
        String HdfsUri= (String) params.get("HdfsUri", "hdfs://localhost:9000");
        String pathHDFSInput= HDFSFileUtils.uploadToHDFS(HdfsUri, pathInputData, pathHDFSTmp);

        //String pathPMML = (String) params.get("pathPMML", "model.xml");

        // create splits of input file:
        //PMML pmml = PMMLUtils.loadPMML(pathPMML);
        //Integer nRecords= Integer.parseInt(params.get("nRecords").toString());
        //MyFileUtils.splitInputFile(pathInputData, tmpInputSplitPath, nRecords);
        
        //Delete pathOutputData and tmpOutputSplitPath if they already exist
        FileUtils.deleteDirectory(new File(pathOutputData));
		//FileUtils.deleteDirectory(new File(tmpOutputSplitPath));

        Model model= PMMLUtils.getModelByName(pmml, params.get("modelName").toString());
        Map<FieldUsageType, List<DerivedField>> fieldMap= PMMLUtils.getDerivedFieldsByUsageType(pmml, model);
        List<DerivedField> activeFields= fieldMap.get(FieldUsageType.ACTIVE);
        List<DerivedField> targetFields= fieldMap.get(FieldUsageType.TARGET);
        DefaultTransformationExecutor executor= new DefaultTransformationExecutor();

        SparkConf conf= new SparkConf().setAppName("spark-norm").setMaster("yarn-client");
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.set("spark.kyro.Registrator", "ml.shifu.norm.MyRegistrator");
        JavaSparkContext jsc= new JavaSparkContext(conf);
        
        /*
        Broadcast<DefaultTransformationExecutor> bexec= jsc.broadcast(executor);
        Broadcast<PMML> bpmml= jsc.broadcast(pmml);
        Broadcast<List<DataField>> bDataFields= jsc.broadcast(pmml.getDataDictionary().getDataFields());
        Broadcast<List<DerivedField>> bActiveFields= jsc.broadcast(activeFields);
        Broadcast<List<DerivedField>> bTargetFields= jsc.broadcast(targetFields);
    	*/
        
        BroadcastVariables bvar= new BroadcastVariables(jsc, executor, pmml, pmml.getDataDictionary().getDataFields(), activeFields, targetFields);
        
        JavaRDD<String> raw= jsc.textFile(pathHDFSInput);
    	JavaRDD<String> normalized= raw.map(new Normalize(bvar));
    	
    	normalized.saveAsTextFile(pathOutputData);
      
    }

}
