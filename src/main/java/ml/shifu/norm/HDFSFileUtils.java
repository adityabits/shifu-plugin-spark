package ml.shifu.norm;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import org.apache.commons.io.FilenameUtils;

import java.nio.file.Path;
import java.nio.file.Paths;


public class HDFSFileUtils {
    
    public static String uploadToHDFS(String localPath, String HDFSDir) {
        FileSystem fs= new FileSystem(new Configuration());
        String basename= Paths.get(localPath).getName().toString(); 
        Path HDFSPath= Paths.get(HDFSDir + "/" + basename);
        // TODO: use a path joiner
        fs.copyFromLocalFile(Paths.get(localPath), HDFSPath);
        return HDFSPath.toString();
    }

}
