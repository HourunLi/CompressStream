import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.net.Socket;
import Utils.*;

public class RLE implements Runnable {

    // public final static int TUPLE_SIZE = 28;
    public final static int TUPLE_NUM_PER_WINDOW = Definitions.TUPLE_NUM_PER_WINDOW;
    public final static int WINDOW_NUM_PER_BATCH = Definitions.WINDOW_NUM_PER_BATCH;//
    public final static int BATCH_NUM = Definitions.BATCH_NUM;
    public final static String DATA_DIR = Definitions.DATA_DIR;
    public final static String DATA_FILE = Definitions.DATA_FILE;
    public final static int START_BATCH = Definitions.START_BATCH;
    public final static int END_BATCH = Definitions.END_BATCH;

    private String name;

    public RLE(String name) {
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
        long size = 0;
        FileInputStream f1;
        DataInputStream d1;
        BufferedReader b1;
        f1 = new FileInputStream(DATA_DIR+DATA_FILE+name+".csv");
        d1 = new DataInputStream(f1);
        b1 = new BufferedReader(new InputStreamReader(d1));

        int[][] col = new int[7][TUPLE_NUM_PER_WINDOW*WINDOW_NUM_PER_BATCH];
        long[] col1 = new long[TUPLE_NUM_PER_WINDOW*WINDOW_NUM_PER_BATCH];
        float[] col2 = new float[TUPLE_NUM_PER_WINDOW*WINDOW_NUM_PER_BATCH];

        int count = 0;
        int batchcount = 0;

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
                StringBuilder sb = new StringBuilder();
                for(int i = 1; i < 7; i++){
                    if(i != 2 && i != 3 && i != 4){
                        startTime = System.nanoTime();
                        if(i == 1)
                            sb = Col_compression.rle(col1);
                        else
                            sb = Col_compression.rle(col[i]);
                        size += sb.toString().length();
                        endTime = System.nanoTime();
                        if(batchcount > START_BATCH && batchcount <= END_BATCH)
                            compress_time += endTime - startTime;
                        startTime = System.nanoTime();
                        bw.writeBytes(sb.toString());
                        bw.writeBytes("\n");
                        endTime = System.nanoTime();
                        if(batchcount > START_BATCH && batchcount <= END_BATCH)
                            trans_time += endTime - startTime;
                        sb.setLength(0);
                    }
                }

                startTime = System.nanoTime();
                for(int i = 0; i < TUPLE_NUM_PER_WINDOW*WINDOW_NUM_PER_BATCH;i++){
                    bw.writeFloat(col2[i]);
                    bw.writeInt(col[3][i]);
                    bw.writeInt(col[4][i]);
                }
                bw.flush();
                endTime = System.nanoTime();
                count = 0;
                if(batchcount > START_BATCH && batchcount <= END_BATCH)
                    trans_time += endTime - startTime;

                if(batchcount == END_BATCH) {
                    System.out.println("size "+ size/END_BATCH);
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
        new Thread(new RLE("1")).start();
        new Thread(new RLE("2")).start();
        // new Thread(new RLE("3")).start();
    }

}
