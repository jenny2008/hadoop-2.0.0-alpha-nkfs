package cn.ict.magicube.fs;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.DelegateToFileSystem;
import org.apache.hadoop.fs.FsConstants;
import org.apache.hadoop.fs.InvalidPathException;
import org.apache.hadoop.fs.Path;

public class NKfs extends DelegateToFileSystem {
	
	NKfs(final URI theUri, final Configuration conf) throws IOException,
			URISyntaxException {
		super(theUri, new NKFileSystem(), conf, "nkfs", false);
	}

	@Override
	public int getUriDefaultPort() {
		return -1; // No default port for file:///
	}
}
