import java.io.File;
import org.testng.Assert;
import org.testng.annotations.Test;
import ml.shifu.norm.SparkModelTransformRequestProcessor;
import ml.shifu.core.request.Request;
import ml.shifu.core.util.JSONUtils;

public class SparkModelTransformRequestProcessorTest {

    @Test
    public void test() throws Exception {
        SparkModelTransformRequestProcessor strp= new SparkModelTransformRequestProcessor();
        Request req=  JSONUtils.readValue(new File("src/test/resources/5_transformexec.json"), Request.class); 
        strp.exec(req);
    }

}
