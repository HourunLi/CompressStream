package Utils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;


public class Row_compression {
	static String ip = "10.0.0.110";
	public static long startTime;
	public static long endTime;
	// static String ip = "192.168.3.36";
	static double ziptime;
	static long size;
	static int count = 0;

    public static String zeros = "0000000000000000000000000000000000000000000000000000000000000000";

    public static String EncodingEliasGammaCoding(int d) {
        d += 1;
        String s = Integer.toBinaryString(d);
        int length = s.length();
        s = zeros.substring(0, length-1) + s;
        return s;

    }

    public static String EncodingEliasDeltaCoding(int d) {
        d += 1;
        String s = Integer.toBinaryString(d);
        int length = s.length();
        String res = EncodingEliasGammaCoding(length-1);
        res = res + s.substring(1);
        // s = zeros.substring(0, length-1) + s;
        return res;

    }
}