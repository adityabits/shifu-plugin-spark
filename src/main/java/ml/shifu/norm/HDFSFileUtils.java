package ml.shifu.norm;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class HDFSFileUtils {
	
	private Configuration hdfsConf;
	FileSystem hdfs;
	HDFSFileUtils(String hadoopConfPath) {
		this.hdfsConf = new Configuration();
		// TODO: use a path joiner(?)
		this.hdfsConf.addResource(new Path(hadoopConfPath + "/" + "core-site.xml"));
		this.hdfsConf.addResource(new Path(hadoopConfPath + "/" + "hdfs-site.xml"));
		try {
			this.hdfs= FileSystem.get(this.hdfsConf);
		} catch (IOException e) {
			System.out.println("Could not create instance of filesystem");
			e.printStackTrace();
		}
	}
	
	public void concat(Path trg, Path[] src) {
		try {
			hdfs.concat(trg, src);
		} catch (IOException e) {
			System.out.println("Failed to concatenate paths to " + trg.toString());
			e.printStackTrace();
		}
	}
	public boolean delete(Path p) {
		System.out.println("Deleting file " + p.toString());
		// deletes files from either local/ hdfs filesystems
		FileSystem fs= null;
		try {
			fs= p.getFileSystem(this.hdfsConf);
		} catch (IOException e1) {
			System.out.println("Cannot obtain FileSystem for " + p.toString());
			e1.printStackTrace();
		} 
		
		try {
			return fs.delete(p, true);
		} catch (IOException e) {
			System.out.println("Cannot delete file " + p.toString());
			e.printStackTrace();
		}
		return false;
	}
	
	public String getURI() {
		return this.hdfs.getUri().toString();
	}
    
    public String uploadToHDFS(String localPath, String HDFSDir) throws Exception {
        FileSystem fs= FileSystem.get(this.hdfsConf);
        String basename= new Path(localPath).getName().toString(); 
        Path HDFSPath= new Path(HDFSDir + "/" + basename);
        // TODO: use a path joiner (?)
        fs.copyFromLocalFile(new Path(localPath), HDFSPath);
        return HDFSPath.toString();
    }

    public String uploadToHDFSIfLocal(String localPath, String HDFSDir) throws Exception {
    	// TODO: better way?
    	if(localPath.startsWith("hdfs:"))
    		return localPath;
        String basename= new Path(localPath).getName().toString(); 
        Path HDFSPath= new Path(HDFSDir + "/" + basename);
        // TODO: use a path joiner (?)
        this.hdfs.copyFromLocalFile(new Path(localPath), HDFSPath);
        return relativeToFullHDFSPath(HDFSPath.toString());
    }
    
    public String relativeToFullHDFSPath(String relPath) {
    	if(relPath.startsWith("hdfs:") || relPath.startsWith("file:"))
    		return relPath;
    	if(relPath.startsWith("/")) {
    		// relPath relative to root
    		return this.hdfs.getUri().toString() + relPath;
    	}
    	else {
    		// assume that path is relative to home
    		return this.hdfs.getHomeDirectory().toString() + "/" + relPath;
    	}
    }
    
    public boolean deleteFile(String pathStr) {
    	
    	Path path= new Path(pathStr);
    	boolean retValue = false;
    	try {
			FileSystem fs= path.getFileSystem(this.hdfsConf);
			retValue= fs.delete(path, false);
		} catch (IOException e) {
			e.printStackTrace();
		}
    	return retValue;
    }
}
