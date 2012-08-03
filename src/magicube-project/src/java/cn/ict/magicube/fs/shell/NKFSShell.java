package cn.ict.magicube.fs.shell;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.LinkedList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.shell.Command;
import org.apache.hadoop.fs.shell.CommandFactory;
import org.apache.hadoop.fs.shell.PathData;
import org.apache.hadoop.util.ToolRunner;

import cn.ict.magicube.fs.NKFileSystem;
import cn.ict.magicube.fs.NKFileSystem.NKPathTranslator;
import cn.ict.magicube.fs.mapreduce.NKFSFixer;
import cn.ict.magicube.fs.mapreduce.NKFSTransformer;
import cn.ict.magicube.fs.PartInfo;
import cn.ict.magicube.fs.PathUtils;

public class NKFSShell extends FsShell {

	static final Log LOG = LogFactory.getLog(NKFSShell.class);

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		FsShell shell = newShellInstance();
		int res;
		try {
			res = ToolRunner.run(shell, args);
		} finally {
			shell.close();
		}
		System.exit(res);
	}

	protected static FsShell newShellInstance() {
		return new NKFSShell();
	}

	protected void registerCommands(CommandFactory factory) {
		factory.registerCommands(Reset.class);
		factory.registerCommands(Unraid.class);
		factory.registerCommands(ShowLocations.class);
		factory.registerCommands(DoRaid.class);
		factory.registerCommands(DoFix.class);
	}

	static {
		Configuration.addDefaultResource("core-site.xml");
	}

	public abstract static class CommandEx extends Command {
		protected void run(Path path) throws IOException {
			throw new RuntimeException("not supposed to get here");
		}
		@Override
		public String getCommandName() {
			return getName();
		}		
	}
	public abstract static class NKFSCommand extends CommandEx {
		protected Configuration _conf = new Configuration();
		protected NKFileSystem _topFS;
		protected FileSystem _baseFS;
		protected void initialize() throws IOException {
			_topFS = new NKFileSystem();
			_topFS.initialize(URI.create("nkfs:///"), _conf);
			_baseFS = _topFS.getBaseFS();			
		}
		@Override
		protected void processOptions(LinkedList<String> args) throws IOException {
			super.processOptions(args);
			initialize();
		}
	}

	public static class DoFix extends CommandEx {
		public static final String NAME = "dofix";
		public static final String USAGE = "";
		public static final String DESCRIPTION = "check and fix corrupted file(s)";
		public static void registerCommands(CommandFactory factory) {
			factory.addClass(DoFix.class, "-dofix");
		}

		protected void processRawArguments(LinkedList<String> args) throws IOException {
			NKFSFixer fixer = new NKFSFixer();
			fixer.doFix();
		}
	}

	
	public static class DoRaid extends CommandEx {
		public static final String NAME = "doraid";
		public static final String USAGE = "";
		public static final String DESCRIPTION = "raid unraided file(s)";
		public static void registerCommands(CommandFactory factory) {
			factory.addClass(DoRaid.class, "-doraid");
		}

		protected void processRawArguments(LinkedList<String> args) throws IOException {
			NKFSTransformer transformer = new NKFSTransformer();
			transformer.doTransform();
		}
	}
	
	public static class ShowLocations extends NKFSCommand {
		public static final String NAME = "showlocations";
		public static final String USAGE = "[<path> ...]";
		public static final String DESCRIPTION = "show locations of file(s)";
		public static void registerCommands(CommandFactory factory) {
			factory.addClass(ShowLocations.class, "-showlocations");
		}
		@Override
		protected void processOptions(LinkedList<String> args) throws IOException {
			super.processOptions(args);
			this.recursive = true;
		}
		@Override
		public void processPath(PathData item) throws IOException {
			if (!(item.fs instanceof NKFileSystem))
				throw new IllegalArgumentException(String.format("%s is not in nkfs", item.path.toUri()));
			out.println("process " + item.stat.getPath());
			if (item.stat.isDirectory())
				return;
			
			out.format("check locations of %s\n", item.path.toUri());
			BlockLocation[] bls = _topFS.getFileBlockLocations(item.path, 0, item.stat.getLen());
			out.format("\treport for topFS application:\n");
			for (BlockLocation bl : bls) {
				out.println("\tblock " + bl.toString());
				for (String h : bl.getNames()) {
					out.format("\t\tdatanode: %s\n", h);
				}
			}
			
			NKPathTranslator ptran = _topFS.new NKPathTranslator(item.path);
			if (!ptran.isRaidedFile()) {
				out.format("\tnot a raided file.\n");
				return;
			}
			
			//Path parityDir = ptran.getParityDirPath();
			PartInfo[] parts = _topFS.getParts(ptran);
			out.format("\tfor raided file: \n");
			for (PartInfo p : parts) {
				out.format("\tpart %d-%d:\n", p.offset, p.length);
				Path partDir = p.getPartDir(_topFS);
				FileStatus[] stats = _baseFS.listStatus(partDir);
				for (FileStatus stat : stats) {
					BlockLocation[] base_bls = _baseFS.getFileBlockLocations(stat.getPath(), 0, stat.getLen());
					out.format("\t\t%s:\n", stat.getPath());
					for (BlockLocation bl : base_bls) {
						out.format("\t\t\tblock %s:\n", bl.toString());
						for (String h : bl.getNames()) {
							out.format("\t\t\t\tdatanode: %s\n", h);
						}
					}
				}
			}
		}
	}
	

	public static class Unraid extends NKFSCommand {
		public static final String NAME = "unraid";
		public static final String USAGE = "[<path> ...]";
		public static final String DESCRIPTION = "Unraid specific file";

		public static void registerCommands(CommandFactory factory) {
			factory.addClass(Unraid.class, "-unraid");
		}

		@Override
		protected void processOptions(LinkedList<String> args) throws IOException {
			super.processOptions(args);
			this.recursive = true;
		}
		@Override
		public void processPath(PathData item) throws IOException {
			if (!(item.fs instanceof NKFileSystem))
				throw new IllegalArgumentException(String.format("%s is not in nkfs", item.path.toUri()));
			FileStatus stat = item.stat;
			out.println("process " + stat.getPath());
			if (stat.isDirectory())
				return;
			NKPathTranslator ptran = _topFS.new NKPathTranslator(stat.getPath());
			if (!ptran.isRaidedFile()) {
				out.format("%s is already a unraided file\n", stat.getPath());
				return;
			}
			out.format("unraiding %s ...\n", stat.getPath());
			Path srcPath = stat.getPath();
			Path dstPath = srcPath.suffix("._COPYING_");
			LOG.debug("copy to " + dstPath.toUri().toString());
			
			byte[] buf = new byte[1024 * 1024];
			InputStream is = null;
			OutputStream os = null;
			try {
				is = _topFS.open(srcPath);				
				os = _topFS.create(dstPath);
				int r = 0;
				while (r >= 0) {
					r = is.read(buf);
					if (r > 0)
						os.write(buf, 0, r);
				}
			} catch (IOException e) {
				e.printStackTrace();
				if (os != null) {
					os.close();
					os = null;
				}
				LOG.debug("delete " + dstPath);
				_topFS.delete(dstPath, false);
				throw e;
			} finally {
				if (is != null)
					is.close();
				if (os != null)
					os.close();
			}
			
			_topFS.delete(srcPath, false);
			_topFS.rename(dstPath, srcPath);
		}
	}

	public static class Reset extends NKFSCommand {
		public static final String NAME = "reset";
		public static final String USAGE = "";
		public static final String DESCRIPTION = 
			"Reset NKFS: remove all infos in nkfs.basedir\n" +
			"WARNING: if not set, nkfs.basedir is \"/\"";
		public static void registerCommands(CommandFactory factory) {
			factory.addClass(Reset.class, "-reset");
		}

		private void deleteDir(String des, Path dir) throws IOException {
			out.format("removing %s directory: %s\n", des, dir.toString());
			_baseFS.delete(dir, true);
		}

		@Override
		protected void processRawArguments(LinkedList<String> args)
		throws IOException {			
			deleteDir("metadata", PathUtils.BASE_METADATA_DIR);
			deleteDir("shadow", PathUtils.BASE_SHADOW_DIR);
			deleteDir("origin", PathUtils.BASE_ORIGIN_DIR);
			deleteDir("parities", PathUtils.BASE_PARITIES_DIR);
			Path jobInputPath = new Path(_conf.get("nkfs.working.file",
					"/tmp/working/in"));
			deleteDir("jobinputpath", jobInputPath);
			Path jobOutputDirPath = new Path(_conf.get("nkfs.working.output.dir",
				"/tmp/working/out"));
			deleteDir("joboutputpath", jobOutputDirPath);
			out.println("nkfs is reset");
		}
	}
}
