package cn.ict.magicube.fs;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import cn.ict.magicube.fs.ParityCreator.Option;


public class CommonParityCreatorProvider extends ParityCreatorProvider {
	static final Log LOG = LogFactory.getLog(CommonParityCreatorProvider.class);
	
	@Override
	public ParityCreator create(FileSystem baseFS, Path partDirPath, Option ... options) {
		if (baseFS == null)
			return null;
		if (partDirPath == null)
			return null;
		return new CommonParityCreator(baseFS, partDirPath);
	}
}
