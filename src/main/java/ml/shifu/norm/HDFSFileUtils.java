package ml.shifu.norm;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class HDFSFileUtils {
    
    public static String uploadToHDFS(String localPath, String HDFSDir) throws IOException {
        FileSystem fs= FileSystem.get(new Configuration());
        String basename= new Path(localPath).getName().toString(); 
        Path HDFSPath= new Path(HDFSDir + "/" + basename);
        // TODO: use a path joiner
        fs.copyFromLocalFile(new Path(localPath), HDFSPath);
        return HDFSPath.toString();
    }

}
