package ml.shifu.norm;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class HDFSFileUtils {
    
    public static String uploadToHDFS(String HdfsUri, String localPath, String HDFSDir) throws Exception {
        FileSystem fs= FileSystem.get(new URI(HdfsUri), new Configuration());
        String basename= new Path(localPath).getName().toString(); 
        Path HDFSPath= new Path(HDFSDir + "/" + basename);
        // TODO: use a path joiner (?)
        fs.copyFromLocalFile(new Path(localPath), HDFSPath);
        return HDFSPath.toString();
    }

}
