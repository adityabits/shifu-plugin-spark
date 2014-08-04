/*
 * This is the main entry point for the spark normalization plugin. The exec method
 * when called with the Request object will:
 * 	1. Unpack paths and other parameters from request object
 * 	2. Create a header file
 * 	3. Upload PMML XML and Request JSON to HDFS
 * 	4. Create a process which calls the spark-submit script, with suitable arguments which include:
 * 		i. The path to the jar file of spark-normalization plugin which contains all dependencies
 * 		ii. The name of the main class which is ml.shifu.norm.SparkNormalizer
 * 		iii. The arguments to SparkNormalizer- HDFS paths to PMML XML and Request JSON
 */

package ml.shifu.norm;

import org.dmg.pmml.*;

import ml.shifu.core.di.spi.RequestProcessor;
import ml.shifu.core.request.Request;
import ml.shifu.core.util.PMMLUtils;
import ml.shifu.core.util.Params;

import java.io.File;
import java.util.List;
import java.lang.ProcessBuilder;
import java.lang.ProcessBuilder.Redirect;
import java.lang.Process;


public class SparkModelTransformRequestProcessor implements RequestProcessor {


    public void exec(Request req) throws Exception {
        Params params= req.getProcessor().getParams();
        String pathPMML= (String) params.get("pathPMML", "model.xml");
        String pathRequest= (String) params.get("pathRequest", "request.xml");
        String pathHDFSTmp= (String) params.get("pathHDFSTmp", "ml/shifu/norm/tmp");
        String pathToJar= (String) params.get("pathToJar"); // default value?
        String HdfsUri= (String)params.get("HdfsUri", "hdfs://localhost:9000");
        System.out.println(HdfsUri);

        // upload PMML XML file to HDFS and get its path
        String pathOutputActiveHeader= params.get("pathOutputActiveHeader").toString();
        PMML pmml= PMMLUtils.loadPMML(pathPMML);
        List<DerivedField> activeFields= CombinedUtils.getActiveFields(pmml, params);
        List<DerivedField> targetFields= CombinedUtils.getTargetFields(pmml, params);        
        CombinedUtils.writeTransformationHeader(pathOutputActiveHeader, activeFields, targetFields);
        String pathHDFSPmml= HDFSFileUtils.uploadToHDFS(HdfsUri, pathPMML, pathHDFSTmp);
        String pathHDFSRequest= HDFSFileUtils.uploadToHDFS(HdfsUri, pathRequest, pathHDFSTmp);
        
        System.out.println(pathHDFSRequest);
        System.out.println(pathHDFSPmml);

        // call spark-submit
        String Spark_submit= (String) params.get("SparkHome") + "/bin/spark-submit";
        System.out.println( Spark_submit);
        ProcessBuilder procBuilder= new ProcessBuilder(Spark_submit, "--class", "ml.shifu.norm.SparkNormalizer", pathToJar, pathHDFSPmml, pathHDFSRequest);
        procBuilder.redirectErrorStream(true);
        File outputFile= new File("log");
        procBuilder.redirectOutput(Redirect.appendTo(outputFile));
        Process proc= procBuilder.start(); 
        proc.waitFor();
    }
    
}
