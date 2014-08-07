/*
 * Contains utils for dealing with HDFS or local filesystems specific to the spark normalization code.
 */
package ml.shifu.plugin.spark;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;


public class HDFSFileUtils {
	
	private Configuration hdfsConf;
	
	HDFSFileUtils(String hadoopConfPath) throws IOException {
		this.hdfsConf = new Configuration();
		// TODO: use a path joiner(?)
		Path coreSitePath= new Path(hadoopConfPath + "/" + "core-site.xml");
		Path hdfsSitePath= new Path(hadoopConfPath + "/" + "hdfs-site.xml");
		this.hdfsConf.addResource(coreSitePath);
		this.hdfsConf.addResource(hdfsSitePath);
		FileSystem hdfs= null;
		try {
			hdfs= FileSystem.get(this.hdfsConf);
		} catch (IOException e) {
			System.out.println("ERROR: Could not create instance of filesystem");
			e.printStackTrace();
		}
		
		if(hdfs instanceof LocalFileSystem) {
			System.out.println("ERROR: Could not create instance of hdfs FileSystem. Please check hadoop configuration files");
			throw new IOException();
		}
		
		if(hdfs != null)
			hdfs.close();
	}
	
	public void concat(Path trg, Path[] src) throws IOException {
		FileSystem hdfs= null;
		try {
			hdfs= FileSystem.get(this.hdfsConf);
			hdfs.concat(trg, src);
		} catch (IOException e) {
			System.out.println("Failed to concatenate paths to " + trg.toString());
			e.printStackTrace();
		}
		hdfs.close();
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
		try {
			if(fs!= null)
				fs.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return true;
	}
	
	public String getURI() throws IOException {
		FileSystem hdfs= FileSystem.get(this.hdfsConf);
		String Uri= hdfs.getUri().toString();
		hdfs.close();
		return Uri;
	}
    
    public String uploadToHDFS(String localPath, String HDFSDir) throws Exception {
        FileSystem fs= FileSystem.get(this.hdfsConf);
        String basename= new Path(localPath).getName().toString(); 
        Path HDFSPath= new Path(HDFSDir + "/" + basename);
        fs.copyFromLocalFile(new Path(localPath), HDFSPath);
        fs.close();
        return HDFSPath.toString();
    }

    public String uploadToHDFSIfLocal(String localPath, String HDFSDir) throws Exception {
    	// TODO: better way?
    	if(localPath.startsWith("hdfs:"))
    		return localPath;
        String basename= new Path(localPath).getName().toString(); 
    	System.out.println("Uploading " + basename + " to HDFS");
        Path HDFSPath= new Path(HDFSDir + "/" + basename);
        // TODO: use a path joiner (?)
        FileSystem hdfs= FileSystem.get(this.hdfsConf);
        hdfs.copyFromLocalFile(new Path(localPath), HDFSPath);
        hdfs.close();
        return relativeToFullHDFSPath(HDFSPath.toString());
    }
    
    public String getHomeDir() throws IOException {
    	FileSystem hdfs= FileSystem.get(this.hdfsConf);
    	String homeDir= hdfs.getHomeDirectory().toString();
    	hdfs.close();
    	return homeDir;
    }
    public String relativeToFullHDFSPath(String relPath) throws IOException {
    	if(relPath.startsWith("hdfs:") || relPath.startsWith("file:"))
    		return relPath;
    	if(relPath.startsWith("/")) {
    		// relPath relative to root
    		return this.getURI() + relPath;
    	}
    	else {
    		// assume that path is relative to home
    		return this.getHomeDir() + "/" + relPath;
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
    
    public void concat(String target, String dirpath, PathFilter filter) throws IllegalArgumentException, IOException {
    	// now concatenate
    	Path targetPath= new Path(target);
    	FileSystem targetFS= targetPath.getFileSystem(this.hdfsConf);
    	FileSystem hdfs= FileSystem.get(this.hdfsConf);
    	targetFS.delete(targetPath, false);
		FileUtil.copyMerge(hdfs, new Path(dirpath), targetFS, new Path(target), true, this.hdfsConf, "");

		targetFS.close();  
		hdfs.close();
    }
    
}
