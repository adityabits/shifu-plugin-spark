package ml.shifu.norm;

import org.apache.commons.io.FileUtils;
import org.dmg.pmml.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

import ml.shifu.core.di.builtin.transform.DefaultTransformationExecutor;
import ml.shifu.core.di.spi.RequestProcessor;
import ml.shifu.core.request.Request;
import ml.shifu.core.util.PMMLUtils;
import ml.shifu.core.util.Params;

import java.io.File;
import java.util.List;
import java.util.Map;
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

        // upload PMML XML file to HDFS and get its path
        String pathOutputActiveHeader= params.get("pathOutputActiveHeader").toString();

        String pathHDFSPmml= HDFSFileUtils.uploadToHDFS(HdfsUri, pathPMML, pathHDFSTmp);
        String pathHDFSRequest= HDFSFileUtils.uploadToHDFS(HdfsUri, pathRequest, pathHDFSTmp);

        // TODO: construct normalize header file

        // call spark-submit
        String Spark_submit= (String) params.get("SparkHome") + "/bin/spark-submit";
        System.out.println( Spark_submit);
        ProcessBuilder procBuilder= new ProcessBuilder(Spark_submit, "--class", "ml.shifu.norm.SparkSubmitter", pathToJar, pathHDFSPmml, pathHDFSRequest);
        procBuilder.redirectErrorStream(true);
        File outputFile= new File("log");
        procBuilder.redirectOutput(Redirect.appendTo(outputFile));
        Process proc= procBuilder.start(); 
        proc.waitFor();
        System.out.println("--------Submitted job");
    }
    
    public void sparkExec(Request req, PMML pmml) throws Exception {

     }
}
