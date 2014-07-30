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
import ml.shifu.core.request.Request;
import ml.shifu.core.util.PMMLUtils;
import ml.shifu.core.util.Params;
import ml.shifu.core.util.JSONUtils;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.fs.Path;

import javax.xml.transform.sax.SAXSource;

public class SparkSubmitter {
    public static void main(String[] args) throws Exception
    {
        // argument 1: HDFS path to request.json
        // argument 2: HDFS path to PMML model.xml
        String pathReq= args[0];
        String pathPMML= args[1];

        FileSystem fs= FileSystem.get(new Configuration());
        InputStream reqInputStream= fs.open(new Path(pathReq));

        // load PMML- include this method in PMMLUtils
        InputStream pmmlInputStream= null;
        PMML pmml= null;
        try {
            pmmlInputStream = fs.open(new Path(pathPMML));
            InputSource source = new InputSource(pmmlInputStream);
            SAXSource transformedSource = ImportFilter.apply(source);
            pmml=  JAXBUtil.unmarshalPMML(transformedSource);
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }

        SparkModelTransformRequestProcessor strp= new SparkModelTransformRequestProcessor();
        Request req=  JSONUtils.readValue(fs.open(new Path(pathReq)), Request.class); 
        strp.sparkExec(req, pmml);
    }

}
