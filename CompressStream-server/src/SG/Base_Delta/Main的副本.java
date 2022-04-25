import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.net.Socket;


import com.lmax.disruptor.*;
import com.lmax.disruptor.dsl.ProducerType;

public class Main {

    public final static int BUFFER_EVENTS_NUM = 1024;
    public final static int CONSUMERS_NUM=1;
    public final static int TUPLE_SIZE = 10;
    public final static int TUPLE_NUM_PER_WINDOW=1024;
    //public final static int WINDOW_NUM_PER_BATCH=150;//
    public final static int WINDOW_NUM_PER_BATCH=100;//
    public final static String DATA_DIR="/Users/zhangyu/Documents/stream/data/";

    static {
        System.load("/Users/zhangyu/Downloads/FineStream/example/Base_Delta/libtest.so");
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

        int[] base = new int[7];
        int[] maxx = new int[7];
        int[] minxx = new int[7];
        int count = 0;
        int batchcount = 0;
        int socketCount = 0;

        String line1;
        boolean hasData = false;
        boolean hasProduce = false;

        while ((line1 = b1.readLine()) != null) {
            // System.out.println(line1);

            String[] split = line1.split(",");
            if(split.length<7)
              continue;
           
            if(count % (TUPLE_NUM_PER_WINDOW*WINDOW_NUM_PER_BATCH) == 0) {
                base[1] = (int)(Long.parseLong(split[1]));
                base[3] = Integer.parseInt(split[3]);
                base[4] = Integer.parseInt(split[4]);
                base[5] = Integer.parseInt(split[5]);
                base[6] = Integer.parseInt(split[6]);
                batchcount++;
                // for(int i = 1; i < 7; i++){
                //     System.out.println(base[i]);
                // }
                // if(batchcount == 1204){
                //     System.out.println("maxx:");
                //     for(int i = 1; i < 7; i++){
                //         System.out.println(maxx[i]);
                //     }
                //     System.out.println("minxx:");
                //     for(int i = 1; i < 7; i++){
                //         System.out.println(minxx[i]);
                //     }
                // }
                // System.exit(1);
            }
            if (buffer.hasRemaining()) {
                // System.out.println(split[1]);
                // buffer.putLong(Long.parseLong(split[1]));/* timestamp */
                // buffer.putFloat(Float.parseFloat(split[2]));/* value */
                // buffer.putInt(Integer.parseInt(split[3]));/* property */
                // buffer.putInt(Integer.parseInt(split[4])); /* plug */
                // buffer.putInt(Integer.parseInt(split[5])); /* household */
                // buffer.putInt(Integer.parseInt(split[6])); /* house */
                // buffer.putInt(Integer.parseInt(split[6])); /* house */
                // for(int i = 1; i < 7; i++){
                //     if(i == 1){
                //         int temp = (int)(Long.parseLong(split[i]) - base[i]);
                //         if(temp > maxx[i])
                //             maxx[i] = temp;
                //         if(temp < minxx[i])
                //             minxx[i] = temp;
                //         if(Long.parseLong(split[i]) - base[i] > 127 || Long.parseLong(split[i]) - base[i] < -128){
                //             System.out.println("exit:" + i);
                //             System.out.println(Long.parseLong(split[i]));
                //             System.out.println(base[i]);
                //             // System.exit(1);
                //         }
                //     }
                //     if(i >= 3){
                //         int temp = (int)(Integer.parseInt(split[i]) - base[i]);
                //         if(temp > maxx[i])
                //             maxx[i] = temp;
                //         if(temp < minxx[i])
                //             minxx[i] = temp;
                //         if(Integer.parseInt(split[i]) - base[i] > 127 || Integer.parseInt(split[i]) - base[i] < -128){
                //             System.out.println("exit:" + i);
                //             System.out.println(Integer.parseInt(split[i]));
                //             System.out.println(base[i]);
                //             // System.exit(1);
                //         }
                //     }
                // }
                buffer.putShort((short)((Long.parseLong(split[1]))-base[1]));
                buffer.putFloat(Float.parseFloat(split[2]));
                buffer.put((byte)(Integer.parseInt(split[3]) - base[3]));
                buffer.put((byte)(Integer.parseInt(split[4]) - base[4]));
                buffer.put((byte)(Integer.parseInt(split[5]) - base[5]));
                buffer.put((byte)(Integer.parseInt(split[6]) - base[6]));
                count += 1;

                // System.out.println(bw.readLine().length());
                // bw.flush();

                // if(count == 1){
                //                     // bw.write(11111);
                // // bw.flush();
                //                 System.exit(1);
                // }

                // socketCount += 1;
                // if(socketCount == 1000){
                //     bw.flush();
                //     socketCount = 0;
                // }


                // System.out.println(split[3]-base[3]);
                // System.out.println((byte)(Integer.parseInt(split[4])-base[4]));
                // System.out.println(split[5]-base[5]);
                // System.out.println(split[6]-base[6]);
            }
            if (buffer.position() == buffer.capacity()) {
                producer.produceData(buffer);
                buffer.clear();
                hasProduce=true;
            }
        }
    }

}

//javac -classpath ./:./disruptor-3.2.1.jar  Main.java
//gcc CBatchHandler.cpp -fPIC -shared -o libtest.so -I/usr/lib/jvm/java-8-openjdk-amd64/include/ -I/usr/lib/jvm/java-8-openjdk-amd64/include/linux
