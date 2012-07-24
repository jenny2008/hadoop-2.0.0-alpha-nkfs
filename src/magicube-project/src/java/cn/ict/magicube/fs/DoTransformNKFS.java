package cn.ict.magicube.fs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.LinkedList;
import java.net.URI;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableFactory;
import org.apache.hadoop.io.SequenceFile.Writer;


public class DoTransformNKFS {
	static final Log LOG = LogFactory.getLog(DoTransformNKFS.class);

	static {
		Configuration.addDefaultResource("core-site.xml");
	}

	public static class BlockInfo implements Writable {
		public Path path;
		public long start;
		public long length;

		
		public BlockInfo(Path p, long s, long l) {
			path = p;
			start = s;
			length = l;
		}
		
		public BlockInfo() {
			path = null;
			start = length = -1L;
		}
		
		//////////////////////////////////////////////////
		// Writable
		//////////////////////////////////////////////////
		static {                                      // register a ctor
			WritableFactories.setFactory
			(BlockInfo.class,
					new WritableFactory() {
				public Writable newInstance() { return new BlockInfo(); }
			});
		}
		@Override
		public void write(DataOutput out) throws IOException {
			Text.writeString(out, path.toUri().toString());
			Text.writeString(out, Long.toString(start));
			Text.writeString(out, Long.toString(length));
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			path = new Path(URI.create(Text.readString(in)));
			start = Long.parseLong(Text.readString(in));
			length = Long.parseLong(Text.readString(in));
		}
		
		@Override
		public String toString() {
			return path.toString() + ": " + Long.toString(start) + " -- +" + Long.toString(length);
		}
	} 

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		NKFileSystem topFS = new NKFileSystem();
		topFS.initialize(URI.create("nkfs:///"), new Configuration());
		FileSystem baseFS = topFS.getBaseFileSystem();
		
		LinkedList<FileStatus> originFiles = new LinkedList<FileStatus>();
		
		NKFileSystem.PathTranslator ptran =
			topFS.new PathTranslator(new Path("/"));
		LOG.info("iterate over " + ptran.getBaseOriginPath());
		
		LinkedList<Path> pendingPath = new LinkedList<Path>();
		pendingPath.add(ptran.getBaseOriginPath());
		while (pendingPath.size() > 0) {
			Path currentPath = pendingPath.pop();
			FileStatus[] status = baseFS.listStatus(currentPath);
			for (FileStatus s : status) {
				if (s.isDirectory())
					pendingPath.add(s.getPath());
				if (s.isFile()) {
					originFiles.add(s);
					LOG.info("XXXXXXXXXXXXX Find File " + s.getPath());
				}
			}
		}
		
		Configuration conf = new Configuration();
		
		/* create an input file for map-reduce job */
		Path jobInputFile = new Path(conf.get("nkfs.working.file", "/tmp/working/in"));
		jobInputFile = new Path(
				new URI(baseFS.getScheme(),
				jobInputFile.toUri().getSchemeSpecificPart(),
				null));
		baseFS.mkdirs(jobInputFile.getParent());
		if (baseFS.exists(jobInputFile))
			throw new Exception("Another NKFS transforming is working. try to remove " + jobInputFile.toString());

		LOG.debug("creating " + jobInputFile);
		FSDataOutputStream out = baseFS.create(jobInputFile);
		///////////////////////////
		SequenceFile.Writer writer = null;
		try {
			writer = SequenceFile.createWriter(conf,
					Writer.stream(out),
					Writer.keyClass(Text.class),
					Writer.valueClass(DoTransformNKFS.BlockInfo.class),
					Writer.compression(SequenceFile.CompressionType.NONE));
			
	        java.util.Collections.shuffle(originFiles);
	        long counter = 0;
	        for (FileStatus stat : originFiles) {
	        	BlockLocation blocks[] = baseFS.getFileBlockLocations(stat, 9, stat.getLen());
	        	for (BlockLocation block : blocks) {
	        		BlockInfo info = new BlockInfo(
	        				baseFS.makeQualified(stat.getPath()), block.getOffset(), block.getLength());
	        		writer.append(new Text(Long.toString(counter)), info);
	        		LOG.info("XXXXXXXXXXXX append to seq file: " + counter + info.toString());
	        	}
	        }
		} finally {
			if (writer != null) {
				writer.close();
        		LOG.info("XXXXXXXXXXX writer closed");
			}
		}
		out.close();

		

		///////////////////////////

		String JobName = "nkfs_transform";				
		baseFS.delete(jobInputFile, false);	
		LOG.info("job finished");

	}

}
