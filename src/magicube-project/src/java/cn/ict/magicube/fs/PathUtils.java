package cn.ict.magicube.fs;

import java.net.URI;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

public class PathUtils {
	public static final Log LOG = LogFactory.getLog(PathUtils.class);

	public static Path BASE_ORIGIN_DIR = null;
	public static Path BASE_SHADOW_DIR = null;
	public static Path BASE_PARITIES_DIR = null;
	public static Path BASE_METADATA_DIR = null;	
	private static String BASE_DIR = null;

	public static Path makeFilePath(Path path) {
		URI uri = path.toUri();
		String uri_s = uri.toString();
		while (uri_s.charAt(uri_s.length() - 1) == Path.SEPARATOR_CHAR) {
			uri_s = uri_s.substring(0, uri_s.length() - 1);
		}
		return new Path(URI.create(uri_s));
	}
	
	public static Path makeDirPath(Path path) {
		URI uri = path.toUri();
		String uri_s = uri.toString();
		if (uri_s.charAt(uri_s.length() - 1) != Path.SEPARATOR_CHAR) {
			URI uri_dir = URI.create(uri_s + Path.SEPARATOR);
			return new Path(uri_dir);
		}
		return path;
	}
	
	public static Path resolvePath(Path p, String r) {
		return new Path(makeDirPath(p).toUri().resolve(r));
	}
	
	public static Path resolveDirPath(Path p, String r) {
		return makeDirPath(resolvePath(p, r));
	}

	public static Path getBaseDirPath(String r) {
		return new Path(
			URI.create(
					BASE_DIR + Path.SEPARATOR + r + Path.SEPARATOR
				)
			);
	}
	
	
	public static Path convertFromBasePath(Path baseRoot, Path p) {
		URI base_root = URI.create(baseRoot.toUri().getPath());
		URI u = URI.create(p.toUri().getPath());
		String rel = Path.SEPARATOR + base_root.relativize(u).getPath();
		return new Path(rel);
	}
	
	public static Path convertFromBaseOrigin(Path p) {
		return convertFromBasePath(BASE_ORIGIN_DIR, p);
	}

	public static Path convertFromBaseParity(Path p) {
		return convertFromBasePath(BASE_PARITIES_DIR, p);
	}
	
	public static Path convertFromBaseParityPart(Path p) {
		//return convertFromBasePath(BASE_PARITIES_DIR, p.getParent().getParent());
		//PartInfo pi = new PartInfo(p);
		//return pi.topFSPath;
		p = makeFilePath(p);
		return convertFromBaseParity(p.getParent());
	}

	public static Path convertFromBaseShadow(Path p) {
		return convertFromBasePath(BASE_SHADOW_DIR, p);
	}
	
	public static Path convertFromBaseMetadata(Path p) {
		return convertFromBasePath(BASE_METADATA_DIR, p);
	}
	
	public static long retriveFileLength(Path lengthFile) {
		String fn = lengthFile.getName();
		String length_str = fn.replaceFirst("length-", "");
		return Long.parseLong(length_str);
	}
	
	public static void initialize(Configuration conf) {
		BASE_DIR = conf.get("nkfs.basedir", "/");
		LOG.debug("BASE_DIR=" + BASE_DIR);
		
		BASE_ORIGIN_DIR = makeDirPath(getBaseDirPath("origin"));
		BASE_SHADOW_DIR = makeDirPath(getBaseDirPath("shadow"));
		BASE_PARITIES_DIR = makeDirPath(getBaseDirPath("parities"));
		BASE_METADATA_DIR = makeDirPath(getBaseDirPath("metadata"));
	}
}
