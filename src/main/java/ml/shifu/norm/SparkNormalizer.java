/*
 * This is the class that is called by spark-submit. Hence, it requires a main method.
 * The two arguments required for this class are the paths to the PMML XML model file and the request JSON file.
 * In the main method, 
 * 	1. The paths and other parameters are unpacked from the request object
 * 	2. SparkConf and JavaSparkContext objects are created 
 * 	3. Broadcast variables are populated
 * 	4. JavaRDD is created from the input file
 * 	5. Map operation is done on the RDD using the Normalize class
 * 	6. resulting JavaRDD is stored as a text file in the input
 */

package ml.shifu.norm;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.dmg.pmml.*;

import ml.shifu.core.di.builtin.transform.DefaultTransformationExecutor;
import ml.shifu.core.request.Request;
import ml.shifu.core.util.Params;
import ml.shifu.core.util.JSONUtils;

import java.util.List;
import org.apache.spark.serializer.KryoSerializer;

import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

public class SparkNormalizer {
    public static void main(String[] args) throws Exception
    {
        // argument 1: HDFS path to request.json
        // argument 2: HDFS path to PMML model.xml
        String pathPMML= args[0];
        String pathReq= args[1];

        FileSystem fs= FileSystem.get(new Configuration());
        PMML pmml= CombinedUtils.loadPMML(pathPMML, fs);        

        Request req=  JSONUtils.readValue(fs.open(new Path(pathReq)), Request.class); 
        
        Params params= req.getProcessor().getParams();

        String pathInputData= params.get("pathInputData").toString();
        String pathOutputData= params.get("pathOutputData").toString();
        String pathHDFSTmp= (String) params.get("pathHDFSTmp", "ml/shifu/norm/tmp");
        String HdfsUri= (String) params.get("HdfsUri", "hdfs://localhost:9000");
        String pathHDFSInput= HDFSFileUtils.uploadToHDFS(HdfsUri, pathInputData, pathHDFSTmp);

        DefaultTransformationExecutor executor= new DefaultTransformationExecutor();

        SparkConf conf= new SparkConf().setAppName("spark-norm").setMaster("yarn-client");
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.set("spark.kyro.Registrator", "ml.shifu.norm.MyRegistrator");
        JavaSparkContext jsc= new JavaSparkContext(conf);
        List<DerivedField> activeFields= CombinedUtils.getActiveFields(pmml, params);
        List<DerivedField> targetFields= CombinedUtils.getTargetFields(pmml, params);
        
        Broadcast<BroadcastVariables> bVar= jsc.broadcast(new BroadcastVariables(executor, pmml, pmml.getDataDictionary().getDataFields(), activeFields, targetFields));
        JavaRDD<String> raw= jsc.textFile(pathHDFSInput);
    	JavaRDD<String> normalized= raw.map(new Normalize(bVar));
    	
    	normalized.saveAsTextFile(pathOutputData);
      
    }

}
