package cn.ict.magicube.fs;

import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.GlobFilter;
import org.apache.hadoop.fs.Path;
import cn.ict.magicube.fs.NKFileSystem.NKPathTranslator;

public class NKFileStatus extends FileStatus {
	
	private static long retriveLength(FileSystem baseFS, FileStatus baseShadowStat,
			NKPathTranslator ptran) {
		if (!baseShadowStat.isFile()) {
			return baseShadowStat.getLen();
		}
		try {
			if (!ptran.isRaidedFile()) {
				FileStatus originStat = baseFS.getFileStatus(ptran.getOriginPath());
				return originStat.getLen();
			}
			Path metaDataPath = ptran.getMetadataDirPath();
			FileStatus[] stats = baseFS.listStatus(metaDataPath, new GlobFilter("length-*"));
			return PathUtils.retriveFileLength(stats[0].getPath());
		} catch (IOException e) {
			e.printStackTrace();
			return -1;
		}

	}	
	public NKFileStatus(NKFileSystem topFS, FileSystem baseFS, FileStatus baseShadowStat,
			Path topFSPath) throws IOException {
		super(NKFileStatus.retriveLength(baseFS, baseShadowStat,
				topFS.new NKPathTranslator(topFSPath)),
				baseShadowStat.isDirectory(),
				baseShadowStat.getReplication(),
				baseShadowStat.getBlockSize(),
				baseShadowStat.getModificationTime(),
				baseShadowStat.getAccessTime(),
				baseShadowStat.getPermission(),
				baseShadowStat.getOwner(),
				baseShadowStat.getGroup(),
				null,
				topFS.new NKPathTranslator(topFSPath).getTopFSPath()
				);

	}
}
