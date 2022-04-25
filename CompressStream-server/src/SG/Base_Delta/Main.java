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



public class Main implements Runnable {
    public final static int BUFFER_EVENTS_NUM = Definitions.BUFFER_EVENTS_NUM;
    public final static int CONSUMERS_NUM = Definitions.CONSUMERS_NUM;
    public final static int TUPLE_SIZE = 10;
    public final static int TUPLE_NUM_PER_WINDOW=Definitions.TUPLE_NUM_PER_WINDOW;
    //public final static int WINDOW_NUM_PER_BATCH=150;//
    public final static int WINDOW_NUM_PER_BATCH=Definitions.WINDOW_NUM_PER_BATCH;//
    // public final static String DATA_DIR="/Users/zhangyu/Documents/stream/data/";

    static {
        System.load("/root/zhangyu/stream/CompressStream-server/example/SG/Base_Delta/libtest.so");
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
        long queryTime = 0;
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

        int[] base = new int[7];
        int[] maxx = new int[7];
        int[] minxx = new int[7];
        int count = 0;
        int batchcount = 0;
        int socketCount = 0;

        String line1;
        boolean hasData = false;
        boolean hasProduce = false;
                
        DataInputStream br = new DataInputStream(new BufferedInputStream(conn.getInputStream()));
        DataOutputStream bw = new DataOutputStream(new BufferedOutputStream(conn.getOutputStream()));
        byte[] bytes = new byte[TUPLE_SIZE * TUPLE_NUM_PER_WINDOW * WINDOW_NUM_PER_BATCH];

        while(true){
            br.readFully(bytes);
            buffer.put(bytes);
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
