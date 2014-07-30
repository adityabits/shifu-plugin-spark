package ml.shifu.norm;

import org.apache.commons.io.FileUtils;
import org.dmg.pmml.*;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;


import com.google.common.base.Joiner;
import com.google.inject.Guice;
import com.google.inject.Injector;

import ml.shifu.core.di.builtin.transform.DefaultTransformationExecutor;
import ml.shifu.core.di.module.SimpleModule;
import ml.shifu.core.di.service.TransformationExecService;
import ml.shifu.core.di.spi.RequestProcessor;
import ml.shifu.core.request.Request;
import ml.shifu.core.util.PMMLUtils;
import ml.shifu.core.util.Params;
import ml.shifu.core.util.JSONUtils;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.lang.ProcessBuilder;
import java.lang.Process;


public class SparkModelTransformRequestProcessor implements RequestProcessor {


    public void exec(Request req) throws Exception {
        Params params= req.getProcessor().getParams();
        String pathPMML= (String) params.get("pathPMML", "model.xml");
        String pathRequest= (String) params.get("pathRequest", "request.xml");
        String pathHDFSTmp= (String) params.get("pathHDFSTmp", "ml/shifu/norm/tmp");
        String pathToJar= (String) params.get("pathToJar"); // default value?

        // upload PMML xml file to HDFS and get its path
        String pathOutputActiveHeader= params.get("pathOutputActiveHeader").toString();

        String pathHDFSPmml= HDFSFileUtils.uploadToHDFS(pathPMML, pathHDFSTmp);
        String pathHDFSRequest= HDFSFileUtils.uploadToHDFS(pathRequest, pathHDFSTmp);

        // TODO: construct normalize header file

        // call spark-submit
        Process proc= new ProcessBuilder("$SPARK_HOME/bin/spark-submit", "--class", "ml.spark.norm.SparkSubmitter", pathToJar, pathHDFSRequest, pathHDFSPmml).start();
        System.out.println("--------Submitted job");
    }
    
    public void sparkExec(Request req, PMML pmml) throws Exception {

        Params params= req.getProcessor().getParams();

        String pathInputData= params.get("pathInputData").toString();
        String pathOutputData= params.get("pathOutputData").toString();
        String pathHDFSInput= HDFSFileUtils.uploadToHDFS(pathInputData, pathHDFSTmp);

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

        SparkConf conf= new SparkConf().setAppName("spark-norm").setMaster("yarn-cluster");
        JavaSparkContext jsc= new JavaSparkContext(conf);
        
        Broadcast<DefaultTransformationExecutor> bexec= jsc.broadcast(executor);
        Broadcast<PMML> bpmml= jsc.broadcast(pmml);
        Broadcast<List<DataField>> bDataFields= jsc.broadcast(pmml.getDataDictionary().getDataFields());
        Broadcast<List<DerivedField>> bActiveFields= jsc.broadcast(activeFields);
        Broadcast<List<DerivedField>> bTargetFields= jsc.broadcast(targetFields);
    	
        JavaRDD<String> raw= jsc.textFile(pathInputData);
    	JavaRDD<String> normalized= raw.map(new Normalize(bpmml, bexec, bDataFields, bActiveFields, bTargetFields));
    	normalized.saveAsTextFile(pathOutputData);
      
    	/*
    	String outputFile;
        for(File inputFile: new File(tmpInputSplitPath).listFiles()) {
        	JavaRDD<String> raw= jsc.textFile(inputFile.toString());
        	JavaRDD<String> normalized= raw.map(new Normalize(bpmml, bexec, bDataFields, bActiveFields, bTargetFields));
        	outputFile= tmpOutputSplitPath + "/" + inputFile.getName();
        	normalized.saveAsTextFile(outputFile);
        }
        
        MyFileUtils.joinOutputFiles(tmpOutputSplitPath, pathOutputData);
        */
    }
}
