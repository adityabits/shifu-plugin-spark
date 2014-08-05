package ml.shifu.norm;
import java.io.File;
import java.io.FilenameFilter;
import java.util.List;

import org.apache.commons.io.filefilter.AbstractFileFilter;
import org.apache.commons.io.filefilter.NameFileFilter;

public class SparkOutputFileNameFilter implements FilenameFilter{

	public boolean accept(File file) {
		if(file.getPath().startsWith("part-") && !file.getPath().endsWith(".crc"))
			return true;
		else
			return false;
	}

	public boolean accept(File file, String name) {
		if(name.startsWith("part-") && !name.endsWith(".crc"))
			return true;
		else
			return false;
	}
}
