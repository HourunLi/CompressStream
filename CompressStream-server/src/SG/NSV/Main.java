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
import java.net.ServerSocket;
import Utils.*;

import com.lmax.disruptor.*;
import com.lmax.disruptor.dsl.ProducerType;



public class Main implements Runnable{

    public final static int BUFFER_EVENTS_NUM = Definitions.BUFFER_EVENTS_NUM;
    public final static int CONSUMERS_NUM = Definitions.CONSUMERS_NUM;
    public final static int TUPLE_SIZE = 28;
    public final static int TUPLE_NUM_PER_WINDOW=Definitions.TUPLE_NUM_PER_WINDOW;
    //public final static int WINDOW_NUM_PER_BATCH=150;//
    public final static int WINDOW_NUM_PER_BATCH=Definitions.WINDOW_NUM_PER_BATCH;//
    public final static String DATA_DIR="/Users/zhangyu/Documents/stream/data/";

    static {
        System.load("/root/zhangyu/stream/CompressStream-server/example/SG/Baseline/libtest.so");
    }

    static native void init();
    static public native void processAStep1(byte[] data);
    static public native void processAStep2();
    static public native void processBStep1(byte[] data);
    static public native void processBStep2();

    Socket socket;

    public Main(Socket socket) {
        this.socket = socket;
    }

    @Override
    public void run() {
        try {
            process(socket);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void process(Socket conn) throws Exception {
        long startTime = 0, endTime = 0;
        long queryTime = 0, decompress_time = 0;
        long stTime = 0,edTime = 0;

        BatchFactory factory = new BatchFactory(TUPLE_SIZE * TUPLE_NUM_PER_WINDOW * WINDOW_NUM_PER_BATCH);
        int bufferSize = BUFFER_EVENTS_NUM;
        ExecutorService es = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        int consumersNum = CONSUMERS_NUM;
        RingBuffer<Batch> ringBuffer = RingBuffer.create(ProducerType.MULTI, factory, bufferSize,
                new BusySpinWaitStrategy());
        SequenceBarrier barriers = ringBuffer.newBarrier();
        BatchHandler[] consumers = new BatchHandler[consumersNum];
        for (int i = 0; i < consumers.length; i++) {
            consumers[i] = new BatchHandler();
        }
        WorkerPool<Batch> workerPool = new WorkerPool<Batch>(ringBuffer, barriers, new ExceptionHandler() {
            @Override
            public void handleEventException(Throwable throwable, long l, Object o) {
            }

            @Override
            public void handleOnStartException(Throwable throwable) {
            }

            @Override
            public void handleOnShutdownException(Throwable throwable) {
            }
        }, consumers);
        ringBuffer.addGatingSequences(workerPool.getWorkerSequences());
        workerPool.start(es);
        BatchProducer producer = new BatchProducer(ringBuffer);
        ByteBuffer buffer = ByteBuffer.allocate(TUPLE_SIZE * TUPLE_NUM_PER_WINDOW * WINDOW_NUM_PER_BATCH);

        String line1;
        boolean hasData = false;
        boolean hasProduce = false;

        // while ((line1 = b1.readLine()) != null) {
        //     // System.out.println(line1);

        //     String[] split = line1.split(",");
        //     if(split.length<7)
        //       continue;
           
        //     if (buffer.hasRemaining()) {
        //         buffer.putLong(Long.parseLong(split[1]));/* timestamp */
        //         buffer.putFloat(Float.parseFloat(split[2]));/* value */
        //         buffer.putInt(Integer.parseInt(split[3]));/* property */
        //         buffer.putInt(Integer.parseInt(split[4])); /* plug */
        //         buffer.putInt(Integer.parseInt(split[5])); /* household */
        //         buffer.putInt(Integer.parseInt(split[6])); /* house */
        //     }
        //     if (buffer.position() == buffer.capacity()) {
        //         producer.produceData(buffer);
        //         buffer.clear();
        //         hasProduce=true;
        //     }
        // }
        int count = 0;
        int batchcount = 0;
        int socketCount = 0;

        DataInputStream br = new DataInputStream(new BufferedInputStream(conn.getInputStream()));
        DataOutputStream bw = new DataOutputStream(new BufferedOutputStream(conn.getOutputStream()));
        byte[] bytes = new byte[TUPLE_SIZE * TUPLE_NUM_PER_WINDOW * WINDOW_NUM_PER_BATCH];

        long[][] cols = new long[7][TUPLE_NUM_PER_WINDOW * WINDOW_NUM_PER_BATCH];
        int[] col_count = new int[7];
        float[] valuess = new float[TUPLE_NUM_PER_WINDOW*WINDOW_NUM_PER_BATCH];
        // int[] col3 = new int[TUPLE_NUM_PER_WINDOW*WINDOW_NUM_PER_BATCH];
        // int[] col4 = new int[TUPLE_NUM_PER_WINDOW*WINDOW_NUM_PER_BATCH];

        while(true){
            for(int i = 0; i < TUPLE_NUM_PER_WINDOW*WINDOW_NUM_PER_BATCH; i++){
                cols[1][i] = br.readLong();
                valuess[i] = br.readFloat();
                byte temp = br.readByte();
                stTime = System.nanoTime();
                for(int j = 3; j < 7; j++) {
                    col_count[j] = (temp >> 2 * (6-j)) & 0x03;
                }
                edTime = System.nanoTime();
                if(batchcount >= Definitions.START_BATCH) {
                    decompress_time += edTime - stTime;
                } 
                for(int j = 3; j < 7; j++) {
                    if(col_count[j] == 0) {
                        byte num = br.readByte();
                        stTime = System.nanoTime();
                        cols[j][i] = (long)num;
                        edTime = System.nanoTime();
                        if(batchcount >= Definitions.START_BATCH) {
                            decompress_time += edTime - stTime;
                        } 
                    }
                    else if(col_count[j] == 1) {
                        short num = br.readShort();
                        stTime = System.nanoTime();
                        cols[j][i] = (long)num;
                        edTime = System.nanoTime();
                        if(batchcount >= Definitions.START_BATCH) {
                            decompress_time += edTime - stTime;
                        } 
                    }
                    else if(col_count[j] == 2) {
                        byte num1 = br.readByte();
                        Short num2 = br.readShort();
                        stTime = System.nanoTime();
                        cols[j][i] = (long)(num1 << 16 | num2);
                        edTime = System.nanoTime();
                        if(batchcount >= Definitions.START_BATCH) {
                            decompress_time += edTime - stTime;
                        } 
                    }
                    else if(col_count[j] == 3) {
                        int num = br.readInt();
                        stTime = System.nanoTime();
                        cols[j][i] = (long)num;
                        edTime = System.nanoTime();
                        if(batchcount >= Definitions.START_BATCH) {
                            decompress_time += edTime - stTime;
                        } 
                    }
                }
            }
            // System.out.println(s.length());
            // s = br.readLine();
            // System.out.println(s.length());
            // s = br.readLine();
            // System.out.println(s.length());
            // s = br.readLine();
            // System.out.println(s.length());
            for(int i = 0; i < TUPLE_NUM_PER_WINDOW * WINDOW_NUM_PER_BATCH; i++) {
                buffer.putLong(cols[1][i]);
                buffer.putFloat(valuess[i]);
                buffer.putInt((int)cols[3][i]);
                buffer.putInt((int)cols[4][i]);
                buffer.putInt((int)cols[5][i]);
                buffer.putInt((int)cols[6][i]);
            }
            // System.out.println(cols[1][i]);
            // System.exit(1);
            // buffer.put(bytes);
            batchcount += 1;

            System.out.println(batchcount);

                stTime = System.nanoTime();
                producer.produceData(buffer);
                buffer.clear();
                hasProduce=true;

                edTime = System.nanoTime();
                if(batchcount >= Definitions.START_BATCH) {
                    queryTime += edTime - stTime;
                } 

            for(int i = 1; i < 7; i++)
                col_count[i] = 0;

            bw.write(1);
            bw.flush();

            if(batchcount==Definitions.START_BATCH) {
                startTime = System.nanoTime();
                System.out.println("计时开始");
            }

            if(batchcount == Definitions.END_BATCH){
                endTime = System.nanoTime();
                double dt = (endTime - startTime)/1000000000.0;

                System.out.println("latency: "+dt+" s");
                System.out.println("queryTime: "+queryTime/1000000000.0+" s");
                System.out.println("decom_time: "+decompress_time/1000000000.0+" s");
                break;

            }
            // if(batchcount == 1204)
            //     break;
        }
        bw.close();
        conn.close();
    }
    public static void main(String[] args) throws Exception{

        init();//初始化；
        ServerSocket ss = new ServerSocket(8080);
        while(true) {
            Socket conn = ss.accept();
            System.out.println("Connected");
            new Thread(new Main(conn)).start();
        }
    }
}

//javac -classpath ./:./disruptor-3.2.1.jar  Main.java
//gcc CBatchHandler.cpp -fPIC -shared -o libtest.so -I/usr/lib/jvm/java-8-openjdk-amd64/include/ -I/usr/lib/jvm/java-8-openjdk-amd64/include/linux
