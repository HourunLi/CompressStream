import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.net.Socket;
import Utils.*;

public class Base_Delta implements Runnable{

    public final static int TUPLE_SIZE = 10;
    public final static int TUPLE_NUM_PER_WINDOW = Definitions.TUPLE_NUM_PER_WINDOW;
    public final static int WINDOW_NUM_PER_BATCH = Definitions.WINDOW_NUM_PER_BATCH;//
    public final static int BATCH_NUM = Definitions.BATCH_NUM;
    public final static String DATA_DIR = Definitions.DATA_DIR;
    public final static String DATA_FILE = Definitions.DATA_FILE;
    public final static int START_BATCH = Definitions.START_BATCH;
    public final static int END_BATCH = Definitions.END_BATCH;

    private String name;

    public Base_Delta(String name) {
        this.name = name;
    }

    @Override
    public void run() {
        try {
            process(name);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    public static void process(String name) throws Exception {
        long trans_time = 0, compress_time = 0, wait_time = 0, empty_time = 0;
        long startTime = 0, endTime = 0;
        FileInputStream f1;
        DataInputStream d1;
        BufferedReader b1;
        f1 = new FileInputStream(DATA_DIR+DATA_FILE+name+".csv");
        d1 = new DataInputStream(f1);
        b1 = new BufferedReader(new InputStreamReader(d1));

        int[] base = new int[7];
        int count = 0;
        int batchcount = 0;
        int[][] col = new int[7][TUPLE_NUM_PER_WINDOW*WINDOW_NUM_PER_BATCH];
        long[] col1 = new long[TUPLE_NUM_PER_WINDOW*WINDOW_NUM_PER_BATCH];
        float[] col2 = new float[TUPLE_NUM_PER_WINDOW*WINDOW_NUM_PER_BATCH];

        int[] BDDomain = Definitions.BDDomain;

        String line1;

        Socket conn = new Socket(Definitions.ip, 8080);
        DataOutputStream bw = new DataOutputStream(new BufferedOutputStream(conn.getOutputStream()));
        DataInputStream br = new DataInputStream(new BufferedInputStream(conn.getInputStream()));

        while ((line1 = b1.readLine()) != null) {
            // System.out.println(line1);

            String[] split = line1.split(",");
            if(split.length<7)
              continue;
           
            if(count == 0) {
                batchcount++;
                System.out.println(batchcount);
            }
            if (count < TUPLE_NUM_PER_WINDOW*WINDOW_NUM_PER_BATCH) {
                startTime = System.nanoTime();
                endTime = System.nanoTime();
                if(batchcount > START_BATCH && batchcount <= END_BATCH)
                    empty_time += endTime - startTime;
                
                startTime = System.nanoTime();
                col1[count] = Long.parseLong(split[1]);
                col2[count] = Float.parseFloat(split[2]);
                col[3][count] = Integer.parseInt(split[3]);
                col[4][count] = Integer.parseInt(split[4]);
                col[5][count] = Integer.parseInt(split[5]);
                col[6][count] = Integer.parseInt(split[6]);
                count += 1;
                endTime = System.nanoTime();
                if(batchcount > START_BATCH && batchcount <= END_BATCH)
                    wait_time += endTime - startTime;
            }
            if (count == TUPLE_NUM_PER_WINDOW*WINDOW_NUM_PER_BATCH) {
                startTime = System.nanoTime();
                for(int i = 1; i < 7; i++) {
                    if(i == 1) {
                        col[1] = Col_compression.Base_Delta(col1);
                    }
                    else if(i >= 3) {
                        col[i] = Col_compression.Base_Delta(col[i]);
                    }
                }
                endTime = System.nanoTime();
                if(batchcount > START_BATCH && batchcount <= END_BATCH)
                    compress_time += endTime - startTime;
                // bw.writeByte(1);
                startTime = System.nanoTime();
                for(int i = 0; i < TUPLE_NUM_PER_WINDOW*WINDOW_NUM_PER_BATCH; i++) {
                    for(int j = 1; j < 7; j++) {
                        if(j == 2) {
                            bw.writeFloat(col2[i]);
                        }
                        else {
                            if(BDDomain[j] == 1) {
                                bw.writeByte((byte)col[j][i]);
                            } 
                            if(BDDomain[j] == 2) {
                                bw.writeShort((short)col[j][i]);
                            }
                            if(BDDomain[j] == 3) {
                                bw.writeByte((byte)(col[j][i]) >> 16);
                                bw.writeShort((short)col[j][i]);
                            }
                            if(BDDomain[j] == 4) {
                                bw.writeInt(col[j][i]);
                            }
                        }
                    }
                }
                bw.flush();
                endTime = System.nanoTime();
                count = 0;
                if(batchcount > START_BATCH && batchcount <= END_BATCH)
                    trans_time += endTime - startTime;
                // bw.close();
                // bw.writeByte((byte)(-111));
                // while(true){
                //     if(br.read() == 1){
                //         System.out.println(batchcount);
                //         break;
                //     }
                // }
                if(batchcount == END_BATCH) {
                    System.out.println("trans_time "+ trans_time/1000000000.0 + " s");
                    System.out.println("compress_time "+ compress_time/1000000000.0 + " s");
                    System.out.println("wait_time "+ (wait_time - empty_time)/1000000000.0 + " s");
                }
                if(batchcount == BATCH_NUM) {
                    break;
                }
            }
        }
                br.close();
                bw.close();
                conn.close();
    }

    public static void main(String[] args) {
        new Thread(new Base_Delta("1")).start();
        new Thread(new Base_Delta("2")).start();
        // new Thread(new Base_Delta("3")).start();
    }

}
