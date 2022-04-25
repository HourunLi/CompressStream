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

public class Elias_Delta implements Runnable {

    // public final static int TUPLE_SIZE = 28;
    public final static int TUPLE_NUM_PER_WINDOW = Definitions.TUPLE_NUM_PER_WINDOW;
    public final static int WINDOW_NUM_PER_BATCH = Definitions.WINDOW_NUM_PER_BATCH;//
    public final static int BATCH_NUM = Definitions.BATCH_NUM;
    public final static String DATA_DIR = Definitions.DATA_DIR;
    public final static String DATA_FILE = Definitions.DATA_FILE;
    public final static int START_BATCH = Definitions.START_BATCH;
    public final static int END_BATCH = Definitions.END_BATCH;

    private String name;

    public Elias_Delta(String name) {
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

        int count = 0;
        int batchcount = 0;
        String[] s = new String[7];

        int[] EDDomain = Definitions.EDDomain;

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

                count += 1;
                long col1 = Long.parseLong(split[1]);
                float col2 = Float.parseFloat(split[2]);
                int col3 = Integer.parseInt(split[3]);
                int col4 = Integer.parseInt(split[4]);
                int col5 = Integer.parseInt(split[5]);
                int col6 = Integer.parseInt(split[6]);

                startTime = System.nanoTime();
                bw.writeLong(col1);
                bw.writeFloat(col2);
                endTime = System.nanoTime();
                if(batchcount > START_BATCH && batchcount <= END_BATCH)
                    trans_time += endTime - startTime;

                startTime = System.nanoTime();
                s[3] = Row_compression.EncodingEliasDeltaCoding(col3);
                s[4] = Row_compression.EncodingEliasDeltaCoding(col4);
                s[5] = Row_compression.EncodingEliasDeltaCoding(col5);
                s[6] = Row_compression.EncodingEliasDeltaCoding(col6);
                endTime = System.nanoTime();
                if(batchcount > START_BATCH && batchcount <= END_BATCH)
                    compress_time += endTime - startTime;
                
                startTime = System.nanoTime();
                for(int i = 3; i < 7; i++){
                    if(EDDomain[i] == 1) {
                        bw.writeByte((byte)Integer.parseInt(s[i],2));
                    } 
                    if(EDDomain[i] == 2) {
                        bw.writeShort((short)Integer.parseInt(s[i],2));
                    }
                    if(EDDomain[i] == 3) {
                        bw.writeByte((byte)(Integer.parseInt(s[i],2) >> 16));
                        bw.writeShort((short)Integer.parseInt(s[i],2));
                    }
                    if(EDDomain[i] == 4) {
                        bw.writeInt(Integer.parseInt(s[i],2));
                    }
                }
                endTime = System.nanoTime();
                if(batchcount > START_BATCH && batchcount <= END_BATCH)
                    trans_time += endTime - startTime;
            }
            if (count == TUPLE_NUM_PER_WINDOW*WINDOW_NUM_PER_BATCH) {
                // bw.writeByte(1);
                startTime = System.nanoTime();
                bw.flush();
                endTime = System.nanoTime();
                count = 0;
                if(batchcount > START_BATCH && batchcount <= END_BATCH)
                    trans_time += endTime - startTime;

                if(batchcount == END_BATCH) {
                    System.out.println("trans_time "+ (trans_time - 2*empty_time)/1000000000.0 + " s");
                    System.out.println("compress_time "+ (compress_time - empty_time)/1000000000.0 + " s");
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
        new Thread(new Elias_Delta("1")).start();
        new Thread(new Elias_Delta("2")).start();
        // new Thread(new Elias_Delta("3")).start();
    }

}
