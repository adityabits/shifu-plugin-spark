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
 *
 *
 * Parameters required in Request object:
 * 	1. Path of PMML XML- can be either local or HDFS
 * 	2. Path of Request object- can be either local or HDFS
 * 	3. Input file path- local/ hdfs
 * 	4. Output file path- local/ hdfs
 * 	5. HDFS temp directory path- MUST be HDFS
 * 	6. 
 */

package ml.shifu.plugin.spark;

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

import org.apache.hadoop.fs.Path;

public class SparkModelTransformRequestProcessor implements RequestProcessor {


    public void exec(Request req) throws Exception {
        Params params= req.getProcessor().getParams();
        String pathPMML= (String) params.get("pathPMML", "model.xml");
        String pathRequest= (String) params.get("pathRequest", "request.xml");
        String pathHDFSTmp= (String) params.get("pathHDFSTmp", "ml/shifu/norm/tmp");
        String pathToJar= (String) params.get("pathToJar"); // default value?
        String pathHadoopConf= (String)params.get("pathHadoopConf", "/usr/local/hadoop/etc/hadoop");
        String pathOutputActiveHeader= params.get("pathOutputActiveHeader").toString();
        String pathOutputData= params.get("pathOutputData").toString();
        String pathInputData= params.get("pathInputData").toString();
        
        HDFSFileUtils hdfsUtils= new HDFSFileUtils(pathHadoopConf);
        pathHDFSTmp= hdfsUtils.relativeToFullHDFSPath(pathHDFSTmp);
        
        // delete output file and hdfs tmp file's output folder
        hdfsUtils.delete(new Path(pathOutputData));
        hdfsUtils.delete(new Path(pathHDFSTmp + '/' + "output"));
        
        
        // upload PMML.xml and Request.json to HDFS if on local FS
        String pathHDFSPmml= hdfsUtils.uploadToHDFSIfLocal(pathPMML, pathHDFSTmp);
        String pathHDFSRequest= hdfsUtils.uploadToHDFSIfLocal(pathRequest, pathHDFSTmp);
        String pathHDFSInput= hdfsUtils.uploadToHDFSIfLocal(pathInputData, pathHDFSTmp);
        
        String hdfsUri= hdfsUtils.getURI();
        
                
        PMML pmml= PMMLUtils.loadPMML(pathPMML);
		List<DerivedField> activeFields= CombinedUtils.getActiveFields(pmml, params);
        List<DerivedField> targetFields= CombinedUtils.getTargetFields(pmml, params);        
        CombinedUtils.writeTransformationHeader(pathOutputActiveHeader, activeFields, targetFields);
        
        // call spark-submit
        String Spark_submit= (String) params.get("SparkHome") + "/bin/spark-submit";
        System.out.println( Spark_submit);
        ProcessBuilder procBuilder= new ProcessBuilder(Spark_submit, "--class", "ml.shifu.plugin.spark.SparkNormalizer", pathToJar, hdfsUri, pathHDFSInput, pathHDFSPmml, pathHDFSRequest);
        procBuilder.redirectErrorStream(true);
        File outputFile= new File("log");
        procBuilder.redirectOutput(Redirect.appendTo(outputFile));
        Process proc= procBuilder.start(); 
        proc.waitFor();
        System.out.println("Job complete, now concatenating files");
        System.out.println("pathHDFSTmp= " + pathHDFSTmp);
        // now concatenate all files into a single file on HDFS
        hdfsUtils.concat(pathOutputData, pathHDFSTmp + "/output", new SparkOutputFileNameFilter());
        
    }
    
}
