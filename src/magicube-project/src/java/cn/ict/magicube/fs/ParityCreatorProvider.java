package cn.ict.magicube.fs;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public abstract class ParityCreatorProvider {
	public abstract ParityCreator create(
			FileSystem baseFS,
			Path partDirPath,
			ParityCreator.Option ... options);
}
