package cn.ict.magicube.fs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableFactory;
import org.apache.hadoop.io.Text;
import cn.ict.magicube.fs.NKFileSystem.NKPathTranslator;

public class PartInfo implements Writable, Comparable<PartInfo> {
	static final Log LOG = LogFactory.getLog(PartInfo.class);

	public class ParityInfo {
		public final int _parityNum;
		private final NKFileSystem _topFS;
		private final FileSystem _baseFS;
		public ParityInfo(NKFileSystem topFS, int parityNum) {
			_parityNum = parityNum;
			_topFS = topFS;
			_baseFS = topFS.getBaseFS();
		}
		
		public PartInfo getOwner() {
			return PartInfo.this;
		}
		
		public Path getParityFile() {
			return PartInfo.this.getParityFile(_topFS, _parityNum);
		}
		
		public Path getCorrespondingOriginFile() {
			return PartInfo.this.getOriginFile(_topFS);
		}
		
		public boolean exist() throws IOException {
			return PartInfo.this.checkParityFile(_topFS, _parityNum); 
		}
		
		public String toString() {
			return String.format("parity %d for %s (%s)",
					_parityNum,
					PartInfo.this.qualifiedTopFSPath,
					getParityFile().toUri()
					);
		}
	}
	
	// source file in topFS
	public Path qualifiedTopFSPath;
	public long offset;
	public long length;

	// path of coding source
	public Path srcPath;
	public boolean isSrcTopPath;
	public long srcOffset;

	//////////////////////////////////////////////////
	// Writable
	//////////////////////////////////////////////////
	static { // register a ctor
		WritableFactories.setFactory
		(PartInfo.class,
				new WritableFactory() {
			public Writable newInstance() { return new PartInfo(); }
		});
		Configuration.addDefaultResource("core-site.xml");
	}
	
	public ParityInfo[] getExistingParities(NKFileSystem topFS) throws FileNotFoundException, IOException {
		Path partDir = getPartDir(topFS);
		FileStatus[] stats = topFS.getBaseFS().listStatus(partDir);
		ArrayList<ParityInfo> lst = new ArrayList<ParityInfo>();
		for (FileStatus s : stats) {
			Path p = s.getPath();
			int pn = PathUtils.retriveParityNum(p);
			if (pn <= 0)
				continue;
			ParityInfo pi = new ParityInfo(topFS, pn);
			lst.add(pi);
		}
		ParityInfo[] res = new ParityInfo[lst.size()];
		return lst.toArray(res);
	}
	
	public PartInfo(Path qualifiedTopFSPath, long offset, long length) {
		this.qualifiedTopFSPath = new Path(URI.create(qualifiedTopFSPath.toUri().getPath()));
		this.offset = offset;
		this.length = length;
		this.srcPath = qualifiedTopFSPath;
		this.isSrcTopPath = true;
		this.srcOffset = offset;
	}
	
	public PartInfo(Path qualifiedTopFSPath, long offset, long length,
			Path baseSrcPath, long baseSrcOffset) {
		this(qualifiedTopFSPath, offset, length);
		this.srcPath = baseSrcPath;
		this.srcOffset = baseSrcOffset;
		this.isSrcTopPath = false;
	}
	
	// partDir should be /nkfs_base/parities/data/test/part-0-12345/
	public PartInfo(Path qualifiedPartDir) {
		Path p = PathUtils.makeFilePath(qualifiedPartDir);
		String partName = p.getName();
		Pattern pattern = Pattern.compile("part-(\\d+)-(\\d+)");
		Matcher m = pattern.matcher(partName);
		
		if (!m.find()) {
			throw new IllegalArgumentException(String.format(
					"unable to explain part dir %s",
					qualifiedPartDir));
		}

		this.offset = Long.parseLong(m.group(1));
		this.length = Long.parseLong(m.group(2));
		
		this.qualifiedTopFSPath = PathUtils.convertFromBaseParityPart(qualifiedPartDir);
		
		this.srcPath = this.qualifiedTopFSPath;
		this.srcOffset = offset;
		this.isSrcTopPath = true;
	}
	
	public PartInfo(Path partDir, Path baseSrcPath, long baseSrcOffset) {
		this(partDir);
		
		this.srcPath = baseSrcPath;
		this.srcOffset = baseSrcOffset;
		this.isSrcTopPath = false;
	}
	
	public PartInfo() {
		qualifiedTopFSPath = srcPath = null;
		offset = length = srcOffset = -1;
		isSrcTopPath = false;
	}
	
	public PartInfo(PartInfo p) {
		this.qualifiedTopFSPath = p.qualifiedTopFSPath;
		this.offset = p.offset;
		this.length = p.length;
		this.srcPath = p.srcPath;
		this.isSrcTopPath = p.isSrcTopPath;
		this.srcOffset = p.srcOffset;
	}
	
	public Path getPartDir(NKFileSystem topFS) {
		NKPathTranslator ptran = topFS.new NKPathTranslator(qualifiedTopFSPath);
		return ptran.getParityPartDirPath(offset, length);
	}
	
	public Path getOriginFile(NKFileSystem topFS) {
		Path partDir = getPartDir(topFS);
		return PathUtils.resolvePath(partDir, "origin");
	}
	
	public Path getParityFile(NKFileSystem topFS, int parityNum) {
		Path partDir = getPartDir(topFS);
		return PathUtils.resolvePath(partDir,
				String.format("parity_%d", parityNum));
	}
	
	public boolean checkParityFile(NKFileSystem topFS, int parityNum) throws IOException {
		return topFS.getBaseFS().exists(getParityFile(topFS, parityNum));
	}
	
	public static int retriveParityNum(Path parityFilePath) {
		parityFilePath = PathUtils.makeFilePath(parityFilePath);
		String parityFileName = parityFilePath.getName();
		Pattern pattern = Pattern.compile("parity_(\\d+)");
		Matcher m = pattern.matcher(parityFileName);
		
		if (!m.find()) {
			throw new IllegalArgumentException(String.format(
					"unable to explain parity file path %s",
					parityFilePath));
		}
		return Integer.parseInt(m.group(1));
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		Text.writeString(out, qualifiedTopFSPath.toUri().toString());
		Text.writeString(out, Long.toString(offset));
		Text.writeString(out, Long.toString(length));
		Text.writeString(out, srcPath.toUri().toString());
		Text.writeString(out, Long.toString(srcOffset));
		Text.writeString(out, Boolean.toString(isSrcTopPath));
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		qualifiedTopFSPath = new Path(URI.create(Text.readString(in)));
		offset = Long.parseLong(Text.readString(in));
		length = Long.parseLong(Text.readString(in));
		srcPath = new Path(URI.create(Text.readString(in)));
		srcOffset = Long.parseLong(Text.readString(in));
		isSrcTopPath = Boolean.parseBoolean(Text.readString(in));
	}
	
	public static void main(String[] args) throws IOException {
		Configuration conf = new Configuration();
		PathUtils.initialize(conf);
		NKFileSystem topFS = (NKFileSystem)FileSystem.get(URI.create("nkfs:///"), conf);
		topFS.initialize(URI.create("nkfs:///"), conf);
		//topFS.setWorkingDirectory(new Path("/datax"));
		PartInfo p1 = new PartInfo(
				(new Path(URI.create("nkfs:///data/test"))), 0, 12345);
		LOG.info(p1.getOriginFile(topFS));
		LOG.info(p1.getParityFile(topFS, 4));
		LOG.info(p1.getPartDir(topFS));
		LOG.info("-------");
		PartInfo p2 = new PartInfo(p1.getPartDir(topFS));
		LOG.info(p2.getParityFile(topFS, 4));
		LOG.info(p2.getPartDir(topFS));
		LOG.info(PartInfo.retriveParityNum(p2.getParityFile(topFS, 12)));
	}

	@Override
	public int compareTo(PartInfo o) {
		if (!this.qualifiedTopFSPath.equals(o.qualifiedTopFSPath)) {
			throw new IllegalArgumentException(
					String.format(
					"Attemps to compare part of different file: %s vs. %s",
					this.toString(), o.toString()));
		}
		long diff = this.offset - o.offset;
		if (diff == 0)
			return 0;
		if (diff > 0)
			return 1;	
		return -1;
	}
	
	private String getDesc() {
		return String.format("part:%s-%d-%d",
				qualifiedTopFSPath, offset, length);
	}
	
	@Override
	public boolean equals(Object o) {
		if (!(o instanceof PartInfo)) {
			return false;
		}
		PartInfo po = (PartInfo)o;
		return this.getDesc().equals(po.getDesc());
	}
	
	@Override
	public String toString() {
		return String.format("part:%s-%d-%d:src:%s-%d-%d(%s)",
				qualifiedTopFSPath, offset, length,
				srcPath, srcOffset, length,
				Boolean.toString(isSrcTopPath));
	}
	
	@Override
	public int hashCode() {
		return this.getDesc().hashCode();
	}
}
