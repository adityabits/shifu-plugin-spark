package ml.shifu.plugin.spark;
import java.io.File;
import java.io.FilenameFilter;
import java.util.List;

import org.apache.commons.io.filefilter.AbstractFileFilter;
import org.apache.commons.io.filefilter.NameFileFilter;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

public class SparkOutputFileNameFilter implements PathFilter{

	public boolean accept(Path path) {
		String name= path.getName();
		if(name.startsWith("part-") && !name.endsWith(".crc"))
			return true;
		else
			return false;
	}
}
