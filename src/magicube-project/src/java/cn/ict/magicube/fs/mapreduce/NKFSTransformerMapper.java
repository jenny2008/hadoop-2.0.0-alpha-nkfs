package cn.ict.magicube.fs.mapreduce;

import java.io.IOException;
import java.io.OutputStream;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import cn.ict.magicube.fs.ParityCreator;
import cn.ict.magicube.fs.PartInfo;
import cn.ict.magicube.math.RaidAlgorithm;
import cn.ict.magicube.math.RaidNK;


public class NKFSTransformerMapper extends Mapper<Text, PartInfo, Text, Text> {
	static final Log LOG = LogFactory.getLog(NKFSTransformerMapper.class);
	static {
		Configuration.addDefaultResource("core-site.xml");
	}
	
	private NKFSUtil _util;
	private Configuration _conf;
	
	// encoding of key:
	// %d %d %d %d...
	// -1 means origin
	// other indicates parity
	public void map(Text key, PartInfo value, Context context)
		throws IOException, InterruptedException
	{
		LOG.info(String.format("map job: key=%s, value=%s",
				key.toString(), value.toString()));
		_util = new NKFSUtil(context.getConfiguration());
		_conf = _util.conf;
		
		String[] s_ops = key.toString().trim().split(" ");
		
		// layout stuff
		Path partDir = value.getPartDir(_util.topFS);
		LOG.info("partDir=" + partDir.toUri());
		_util.baseFS.mkdirs(partDir);
		
		// create parity creator
		ParityCreator creator = ParityCreator.load(
				ParityCreator.baseFS(_util.baseFS),
				ParityCreator.partDir(partDir)
				);
		
		// create IS and OS
		OutputStream originOS = null;
		OutputStream[] paritiesOS = null;
		FSDataInputStream is = null;

		List<Integer> l_parityNums = new LinkedList<Integer>();
		List<OutputStream> l_paritiesOS = new LinkedList<OutputStream>();
		
		int[] parityNums = null;
		try {	
			if (value.isSrcTopPath) {
				is = _util.topFS.open(value.srcPath);
			} else {
				is = _util.baseFS.open(value.srcPath);
			}
			is.seek(value.srcOffset);
			
			for (String op : s_ops) {
				if (op.equals("0"))
					originOS = creator.createStream("origin");
				else {
					//lst.add(creator.createStream(String.format("parity_%d", op)));
					int n = Integer.parseInt(op);
					l_parityNums.add(n);
					l_paritiesOS.add(creator.createStream(String.format("parity_%d", n)));
				}
			}
			paritiesOS = new OutputStream[l_paritiesOS.size()];
			parityNums = new int[l_parityNums.size()];
			
			l_paritiesOS.toArray(paritiesOS);
			for (int i = 0; i < l_parityNums.size(); i++) {
				parityNums[i] = l_parityNums.get(i);
			}
			
			// encode
			RaidAlgorithm raid = RaidAlgorithm.load(
					RaidAlgorithm.name(_util.raidAlgoName),
					RaidAlgorithm.n(_util.N),
					RaidAlgorithm.k(_util.K)
					);			
			raid.encode(is, paritiesOS, parityNums, value.length, originOS);
			
		} finally {
			if (is != null)
				is.close();
			if (originOS != null)
				originOS.close();
			if (paritiesOS != null) {
				for (OutputStream os : paritiesOS)
					os.close();
			}
		}		
	}
}
