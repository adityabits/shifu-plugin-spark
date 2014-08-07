/*
 * Contains utils for dealing with HDFS or local filesystems specific to the spark normalization code.
 */
package ml.shifu.plugin.spark;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;


public class HDFSFileUtils {
	
	private Configuration hdfsConf;
	FileSystem hdfs;
	
	HDFSFileUtils(String hadoopConfPath) throws IOException {
		this.hdfsConf = new Configuration();
		// TODO: use a path joiner(?)
		Path coreSitePath= new Path(hadoopConfPath + "/" + "core-site.xml");
		Path hdfsSitePath= new Path(hadoopConfPath + "/" + "hdfs-site.xml");
		this.hdfsConf.addResource(coreSitePath);
		this.hdfsConf.addResource(hdfsSitePath);
		try {
			this.hdfs= FileSystem.get(this.hdfsConf);
		} catch (IOException e) {
			System.out.println("Could not create instance of filesystem");
			e.printStackTrace();
		}
		System.out.println("hdfs filesystem= " + this.hdfs.toString());
		System.out.println("reading hdfs conf from " + coreSitePath.toString() + ", " + hdfsSitePath.toString());
		/*
		 * This step gives a "no filesystem found for scheme: hdfs" when run through shifu. 
		FileSystem localFS= FileSystem.get(new Configuration());
		if(localFS.exists(coreSitePath) && localFS.exists(hdfsSitePath))
			System.out.println("Both paths exist!!!");
		else
			System.out.println("At least one path not found");
		localFS.close();
		*/
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
    
    public void concat(String target, String dirpath, PathFilter filter) throws IllegalArgumentException, IOException {
    	// now concatenate
    	Path targetPath= new Path(target);
    	FileSystem targetFS= targetPath.getFileSystem(this.hdfsConf);
    	System.out.println("Target path " + target);
    	System.out.println("Dirpath path " + dirpath);
    	
    	targetFS.delete(targetPath, false);
		System.out.println("target FS- " + targetFS.toString());
		FileUtil.copyMerge(this.hdfs, new Path(dirpath), targetFS, new Path(target), true, this.hdfsConf, "");

		targetFS.close();    	
    }
    
    
	public Path[] listFiles(String path,
			PathFilter filter) {
		FileStatus[] fstatus= null;
		try {
			fstatus= this.hdfs.listStatus(new Path(path), filter);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		Path[] paths= new Path[fstatus.length];
		for(int i= 0; i < fstatus.length; i++) {
			paths[i]= fstatus[i].getPath();
			System.out.println("source path= " + paths[i]);
		}
		
		return paths;
	}
}
