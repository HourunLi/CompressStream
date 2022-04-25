package Utils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;


public class Definitions {
	// public static String ip = "172.17.147.243";
	public static String ip = "47.94.159.31";

    public final static int BUFFER_EVENTS_NUM = 1024;
    public final static int CONSUMERS_NUM=1;
    public final static int TUPLE_SIZE = 28;
    public final static int TUPLE_NUM_PER_WINDOW=1024;
    //public final static int WINDOW_NUM_PER_BATCH=150;//
    public final static int WINDOW_NUM_PER_BATCH=100;//
    public final static int BATCH_NUM = 1000;
    public final static String DATA_DIR="/root/data/";
    // public final static String DATA_DIR="E:\\";
    public final static String DATA_FILE = "SmartGrid/SmartGrid";
    // public final static String DATA_FILE = "SmartGrid";
    public final static int START_BATCH = 3;
    // public final static int END_BATCH = 930;
	public final static int END_BATCH = 13;


	public static long startTime;
	public static long endTime;
	// static String ip = "192.168.3.36";
	static double ziptime;
	static long size;
	static int count = 0;

	public static int[] NumDistinct = {0, 185, 0, 2, 14, 18, 40};
	public static int[] EDDomain = {0, 5, 0, 1, 1, 2, 2};
	public static int[] EGDomain = {0, 5, 0, 1, 1, 2, 2};
	public static int[] ValueDomainMax = {0, 4, 0, 1, 1, 1, 1};
	public static int[] BDDomain = {0, 2, 0, 1, 1, 1, 1};
	public static int[] AverageRunLength = {0, 1664, 0, 0, 1, 5, 40};

	public static String gzip(byte[] data) throws Exception {
		startTime = System.nanoTime();
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		GZIPOutputStream gzip = new GZIPOutputStream(bos);
		gzip.write(data);
		gzip.finish();
		gzip.close();
		byte[] ret = bos.toByteArray();
		// System.out.println(data.length);
		// System.out.println(ret.length);
		// 		endTime = System.nanoTime();
		// double dt = (endTime - startTime) / 1000000000.0;
		// ziptime += dt;
		// System.out.println("ziptime " + ziptime);
		// System.out.println("latency: " + dt + " s");
		// System.exit(1);
		bos.close();
		// return ret;
		return bos.toString("ISO-8859-1");
	}

	public static byte[] ungzip(byte[] data) throws Exception {
		ByteArrayInputStream bis = new ByteArrayInputStream(data);
		GZIPInputStream gzip = new GZIPInputStream(bis);
		byte[] buf = new byte[1024];
		int num = -1;
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		while((num = gzip.read(buf, 0, buf.length)) != -1) {
			bos.write(buf, 0, num);
		}
		gzip.close();
		bis.close();
		byte[] ret = bos.toByteArray();
		bos.flush();
		bos.close();
		return ret;
	}

	public static String compress(String str) throws Exception {
		if(str == null || str.length() == 0) {
			return str;
		}
		count ++;
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		GZIPOutputStream gzip = new GZIPOutputStream(out);
		gzip.write(str.getBytes());
		gzip.finish();
		gzip.close();
		String ret = out.toString("ISO-8859-1");
		out.close();
		return ret;
	}

	public static String decompress(String str) throws Exception {
		if(str == null || str.length() == 0) {
			return str;
		}

		ByteArrayOutputStream out = new ByteArrayOutputStream();
		ByteArrayInputStream in = new ByteArrayInputStream(str.getBytes("ISO-8859-1"));
		GZIPInputStream gunzip = new GZIPInputStream(in);
		byte[] buffer = new byte[1024];
		int n;
		while((n = gunzip.read(buffer)) >= 0) {
			out.write(buffer, 0, n);
		}
		return out.toString();
	}
}