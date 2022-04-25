package Utils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;


public class Col_compression {
	static String ip = "10.0.0.110";
	public static long startTime;
	public static long endTime;
	// static String ip = "192.168.3.36";
	static double ziptime;
	static long size;
	static int count = 0;

	public final static int BUFFER_EVENTS_NUM = Definitions.BUFFER_EVENTS_NUM;
    public final static int CONSUMERS_NUM = Definitions.CONSUMERS_NUM;
    public final static int TUPLE_SIZE = 28;
    public final static int TUPLE_NUM_PER_WINDOW = Definitions.TUPLE_NUM_PER_WINDOW;
    public final static int WINDOW_NUM_PER_BATCH = Definitions.WINDOW_NUM_PER_BATCH;//
    public final static int BATCH_NUM = Definitions.BATCH_NUM;
    public final static String DATA_DIR = Definitions.DATA_DIR;
    public final static String DATA_FILE = Definitions.DATA_FILE;

	public static StringBuilder rle(int[] col)  throws Exception {
		StringBuilder sb = new StringBuilder();
		int rle_count = 0;
		int last = col[0];
		int col_count = 0;

		for(int i = 0; i < col.length; i++) {
            if(last == col[i]) {
                col_count ++;
            }
            else {
                sb = sb.append(String.valueOf(last) + "," + String.valueOf(col_count) + ",");
                last = col[i];
                col_count = 1;
            }
        }
        sb.append(String.valueOf(last) + "," + String.valueOf(col_count) + ",");
        return sb;
    }

    public static StringBuilder rle(long[] col)  throws Exception {
        StringBuilder sb = new StringBuilder();
        int rle_count = 0;
        long last = col[0];
        int col_count = 0;

        for(int i = 0; i < col.length; i++) {
            if(last == col[i]) {
                col_count ++;
            }
            else {
                sb = sb.append(String.valueOf(last) + "," + String.valueOf(col_count) + ",");
                last = col[i];
                col_count = 1;
            }
        }
        sb.append(String.valueOf(last) + "," + String.valueOf(col_count) + ",");
        return sb;
    }

    public static int[] Base_Delta(int[] col) throws Exception {
        int base = col[0];
        int[] result = new int[col.length];
        for(int i = 0; i < col.length; i++) {
            result[i] = col[i] - base;
        }
        return result;

    }

    public static int[] Base_Delta(long[] col) throws Exception {
        long base = col[0];
        int[] result = new int[col.length];
        for(int i = 0; i < col.length; i++) {
            result[i] = (int)(col[i] - base);
        }
        return result;

    }

    public static int[] DICT(int[] col, int NumDistinct) throws Exception {
    	int[] dict = new int[NumDistinct];
    	for(int j = 0; j < NumDistinct; j++) {
    		dict[j] = -1;
    	}
    	int dict_count = 0;
    	int[] result = new int[col.length + NumDistinct];
    	int col_count = 0;
    	for(int i = 0; i < col.length; i++) {
    		int flag = 0;
    		for(int j = 0; j < dict_count; j++) {
    			if(dict[j] == col[i]) {
    				result[col_count++] = j;
    				flag = 1;
    				break;
    			}
    		}
    		if(flag == 0) {
    		    result[col_count++] = dict_count;
    			dict[dict_count++] = col[i];	
    		}
    	}
    	for(int i = 0; i < NumDistinct; i++) {
    		result[col_count++] = dict[i];
    	}
    	return result;
    }

    public static int[] DICT(long[] col, int NumDistinct) throws Exception {
        long[] dict = new long[NumDistinct];
        for(int j = 0; j < NumDistinct; j++) {
            dict[j] = -1;
        }
        int dict_count = 0;
        int[] result = new int[col.length + NumDistinct];
        int col_count = 0;
        for(int i = 0; i < col.length; i++) {
            int flag = 0;
            for(int j = 0; j < dict_count; j++) {
                if(dict[j] == col[i]) {
                    result[col_count++] = j;
                    flag = 1;
                    break;
                }
            }
            if(flag == 0) {
                result[col_count++] = dict_count;
                dict[dict_count++] = col[i];    
            }
        }
        for(int i = 0; i < NumDistinct; i++) {
            result[col_count++] = (int)dict[i];
        }
        return result;
    }

    public static byte[] Bitmap(int[] col, int NumDistinct) throws Exception {
    	int[] bitmap = new int[NumDistinct];
    	for(int j = 0; j < NumDistinct; j++) {
    		bitmap[j] = -1;
    	}
    	int map_count = 0;
    	byte[] result = new byte[col.length / (int)(8/NumDistinct)];
    	int col_count = 0;
    	for(int i = 0; i < col.length;) {
    		byte temp = 0;
    		if(NumDistinct == 1) {
    			for(int k = 7; k >= 0; k--) {
    				int flag = 0;
		    		for(int j = 0; j < map_count; j++) {
		    			if(bitmap[j] == col[i]) {
		    				byte tt = (byte)(1 << j);
		    				temp = (byte)(temp | (tt << k));
		    				flag = 1;
		    				break;
		    			}
		    		}
		    		if(flag == 0) {
		    			byte tt = (byte)(1 << map_count);
		    			temp = (byte)(temp | tt << k);
                        bitmap[map_count] = col[i];
                        map_count++;
		    		}				
    				i++;
    			}
    			result[col_count++] = temp;
    		}
    		else if(NumDistinct == 2) {
    			for(int k = 3; k >= 0; k--) {
    				int flag = 0;
		    		for(int j = 0; j < map_count; j++) {
		    			if(bitmap[j] == col[i]) {
		    				byte tt = (byte)(1 << j);
		    				temp = (byte)(temp | (tt << k*2));
		    				flag = 1;
		    				break;
		    			}
		    		}
		    		if(flag == 0) {
		    			byte tt = (byte)(1 << map_count);
		    			temp = (byte)(temp | (tt << k*2));
                        bitmap[map_count] = col[i];
                        map_count++;
		    		}				
    				i++;
    			}
    			result[col_count++] = temp;
    		}
    		else if(NumDistinct <= 4) {
    			for(int k = 1; k >= 0; k--) {
    				int flag = 0;
		    		for(int j = 0; j < map_count; j++) {
		    			if(bitmap[j] == col[i]) {
		    				byte tt = (byte)(1 << j);
		    				temp = (byte)(temp | (tt << k*4));
		    				flag = 1;
		    				break;
		    			}
		    		}
		    		if(flag == 0) {
		    			byte tt = (byte)(1 << map_count);
		    			temp = (byte)(temp | (tt << k*4));
                        bitmap[map_count] = col[i];
                        map_count++;
		    		}				
    				i++;
    			}
    			result[col_count++] = temp;
    		}
    		else if(NumDistinct <= 8) {
    			for(int k = 0; k >= 0; k--) {
    				int flag = 0;
		    		for(int j = 0; j < map_count; j++) {
		    			if(bitmap[j] == col[i]) {
		    				byte tt = (byte)(1 << j);
		    				temp = (byte)(temp | tt);
		    				flag = 1;
		    				break;
		    			}
		    		}
		    		if(flag == 0) {
		    			byte tt = (byte)(1 << map_count);
		    			temp = (byte)(temp | tt);
                        bitmap[map_count] = col[i];
                        map_count++;
		    		}				
    				i++;
    			}
    			result[col_count++] = temp;
    		}
    	}
    	for(int i = 0; i < NumDistinct; i++) {
    		// result[col_count++] = bitmap[i];
    	}
    	return result;

    }

}