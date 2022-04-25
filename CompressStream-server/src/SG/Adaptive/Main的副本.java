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


import com.lmax.disruptor.*;
import com.lmax.disruptor.dsl.ProducerType;



public class Main {

    public final static int BUFFER_EVENTS_NUM = 1024;
    public final static int CONSUMERS_NUM=1;
    public final static int TUPLE_SIZE = 28;
    public final static int TUPLE_NUM_PER_WINDOW=1024;
    //public final static int WINDOW_NUM_PER_BATCH=150;//
    public final static int WINDOW_NUM_PER_BATCH=100;//
    public final static String DATA_DIR="/Users/zhangyu/Documents/stream/data/";

    public static long startTime;
    public static long endTime;

    static {
        System.load("/Users/zhangyu/Downloads/FineStream-online/example/Baseline/libtest.so");
    }

    static native void init();
    static public native void processAStep1(byte[] data);
    static public native void processAStep2();
    static public native void processBStep1(byte[] data);
    static public native void processBStep2();

    public static void main(String[] args) throws Exception {
        init();//初始化；

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
        String datadir = DATA_DIR;
        FileInputStream f1;
        DataInputStream d1;
        BufferedReader b1;
        f1 = new FileInputStream(datadir + "SmartGrid.csv");
        d1 = new DataInputStream(f1);
        b1 = new BufferedReader(new InputStreamReader(d1));

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

        ServerSocket ss = new ServerSocket(8080);
        Socket conn = ss.accept();
        DataInputStream br = new DataInputStream(new BufferedInputStream(conn.getInputStream()));
        DataOutputStream bw = new DataOutputStream(new BufferedOutputStream(conn.getOutputStream()));
        byte[] bytes = new byte[TUPLE_SIZE * TUPLE_NUM_PER_WINDOW * WINDOW_NUM_PER_BATCH];

        while(true){
            // for(int i = 1; i <= TUPLE_SIZE * TUPLE_NUM_PER_WINDOW*WINDOW_NUM_PER_BATCH; i++)
            //     // System.out.println(br.readByte());
            //     br.readByte();
            br.readFully(bytes);
            // for(int i = 1 ; i <= TUPLE_NUM_PER_WINDOW*WINDOW_NUM_PER_BATCH; i++) {
            //     br.readInt();
            // }
            buffer.put(bytes);
            batchcount += 1;

            System.out.println(batchcount);

            producer.produceData(buffer);
            buffer.clear();
            hasProduce=true;

            bw.write(1);
            bw.flush();

            if(batchcount==204) {
                startTime = System.nanoTime();
                System.out.println("计时开始");
            }

            if(batchcount == 704){
                endTime = System.nanoTime();
                double dt = (endTime - startTime)/1000000000.0;

                System.out.println("latency: "+dt+" s");
                System.exit(0);

            }
            // if(batchcount == 1204)
            //     break;
        }
    }
}

//javac -classpath ./:./disruptor-3.2.1.jar  Main.java
//gcc CBatchHandler.cpp -fPIC -shared -o libtest.so -I/usr/lib/jvm/java-8-openjdk-amd64/include/ -I/usr/lib/jvm/java-8-openjdk-amd64/include/linux
