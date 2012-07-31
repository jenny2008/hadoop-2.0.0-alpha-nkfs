package cn.ict.magicube.fs.mapreduce;

import java.io.FileNotFoundException;

import java.io.IOException;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import cn.ict.magicube.fs.PartInfo;
import cn.ict.magicube.fs.PathUtils;
import cn.ict.magicube.fs.NKFileSystem.NKPathTranslator;



public class NKFSTransformer extends NKFSUtil {
	static final Log LOG = LogFactory.getLog(NKFSTransformer.class);

	public NKFSTransformer(Configuration conf) throws IOException {
		super(conf);
	}
	
	public NKFSTransformer() throws IOException {
		super();
	}
	
	private FileStatus[] _originFiles;
	private Path _jobInputPath;
	private Path _jobOutputDirPath;
	private Job _job;
	
	private void getAllOriginFiles() throws IOException {
		LOG.info("iterate over " + PathUtils.BASE_ORIGIN_DIR);

		LinkedList<FileStatus> l = new LinkedList<FileStatus>();
		LinkedList<Path> pendingPath = new LinkedList<Path>();
		pendingPath.add(PathUtils.BASE_ORIGIN_DIR);
		while (pendingPath.size() > 0) {
			Path currentPath = pendingPath.pop();
			FileStatus[] status = baseFS.listStatus(currentPath);
			for (FileStatus s : status) {
				if (s.isDirectory())
					pendingPath.add(s.getPath());
				if (s.isFile()) {
					l.add(s);
					LOG.debug("Find File " + s.getPath());
				}
			}
		}
		java.util.Collections.shuffle(l);
		_originFiles = new FileStatus[l.size()];
		l.toArray(_originFiles);
	}

	private void createJobFile() throws Exception {
		int parityShift = conf.getInt("nkfs.parity.coding.shift", 3);
		baseFS.mkdirs(_jobInputPath.getParent());
		if (baseFS.exists(_jobInputPath))
			throw new Exception("Another NKFS transforming is working. try to remove " + _jobInputPath.toString());
		LOG.debug("creating " + _jobInputPath);
		FSDataOutputStream out = baseFS.create(_jobInputPath);
		SequenceFile.Writer writer = null;
		try {
			writer = SequenceFile.createWriter(conf,
					Writer.stream(out),
					Writer.keyClass(Text.class),
					Writer.valueClass(PartInfo.class),
					Writer.compression(SequenceFile.CompressionType.NONE));
			
	        for (FileStatus stat : _originFiles) {
	        	BlockLocation blocks[] = baseFS.getFileBlockLocations(stat, 9, stat.getLen());
	        	for (BlockLocation block : blocks) {
	        		Path topFSPath = PathUtils.convertFromBaseOrigin(baseFS.makeQualified(stat.getPath()));
	        		PartInfo info = new PartInfo(
	        				topFSPath,
	        				block.getOffset(), block.getLength(),
	        				baseFS.makeQualified(stat.getPath()),
	        				block.getOffset()
	        				);

	        		StringBuilder keybuilder = new StringBuilder();
	        		keybuilder.append("0");
	        		for (int i = 0; i < N; i++) {
	        			keybuilder.append(" ");
	        			keybuilder.append(Integer.toString(i + parityShift));
	        		}
	        		
	        		writer.append(new Text(keybuilder.toString().trim()), info);
	        		LOG.info("append to seq file: parities : " + info.toString());
	        	}
	        }
		} finally {
			if (writer != null) {
				writer.close();
        		LOG.debug("writer closed");
			}
			out.close();
		}
	}

	static long now() {
		return System.currentTimeMillis();
	}

	private void createJob() throws IOException {
		final SimpleDateFormat dateForm = new SimpleDateFormat("yyyy-MM-dd HH:mm");
		String jobName = "nkfs_transform_" + dateForm.format(new Date(now()));
		Job job = Job.getInstance(conf, jobName);

		job.setSpeculativeExecution(false);
	    job.setJarByClass(NKFSTransformer.class);
	    job.setInputFormatClass(NKFSTransformerInputFormat.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    job.setMapperClass(NKFSTransformerMapper.class);
	    FileInputFormat.setInputPaths(job, _jobInputPath);
	    FileOutputFormat.setOutputPath(job, _jobOutputDirPath);
	    job.setNumReduceTasks(0);
	    _job = job;
	} 
	
	public void doTransform() throws Exception {
		LOG.info("Do transform NKFS");
		getAllOriginFiles();

		_jobInputPath = new Path(conf.get("nkfs.working.file", "/tmp/working/in"));
		_jobInputPath = new Path(
				new URI(baseFS.getScheme(),
				_jobInputPath.toUri().getPath(),
				null));
		_jobOutputDirPath = new Path(conf.get("nkfs.working.output.dir",
					"/tmp/working/out"));
		_jobOutputDirPath = new Path(
				new URI(baseFS.getScheme(),
						_jobOutputDirPath.toUri().getPath(),
						null));
		if (baseFS.exists(_jobOutputDirPath))
			baseFS.delete(_jobOutputDirPath, true);

		createJobFile();
		
		for (FileStatus stat : _originFiles) {
			Path topPath = PathUtils.convertFromBaseOrigin(stat.getPath());
			NKPathTranslator ptran = topFS.new NKPathTranslator(topPath);
			topFS.prepareTransform(ptran);
		}

		boolean successful = false;

		try {
			createJob();
			_job.submit();
			successful = _job.waitForCompletion(true);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			baseFS.delete(_jobInputPath, false);
			baseFS.delete(_jobOutputDirPath, true);
		}
		
		LOG.info("job finished: successful=" + Boolean.toString(successful));
		if (successful) {
			LOG.info("remove write permission for original files," +
			"then remove origin part");
			for (FileStatus stat : _originFiles) {
				Path topPath = PathUtils.convertFromBaseOrigin(stat.getPath());
				NKPathTranslator ptran = topFS.new NKPathTranslator(topPath);
				topFS.finishTransform(ptran);
			}
		}
	}
	
	public static void main(String[] args) throws Exception {
		NKFSTransformer transformer = new NKFSTransformer();
		transformer.doTransform();
	}
}
