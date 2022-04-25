import java.nio.ByteBuffer;

import com.lmax.disruptor.WorkHandler;

public class BatchHandler implements WorkHandler<Batch> {
    public static int count = 0;
    public static long startTime;
    public static long endTime;
    public static boolean GPU = true;
    @Override
    public void onEvent(Batch batch) throws Exception {
      // System.out.println("??");
        //TODO starTime
        if(count==204) {
             startTime = System.nanoTime();
        }
        count++;
// System.out.println(count);
        if(count == 3204){
        //if(count == 505){
            endTime = System.nanoTime();
            double dt = (endTime - startTime)/1000000000.0;

            double data = (batch.getData().capacity()*5)/10485.760;
            //double data = (batch.getData().capacity()*5)/10485.760;

            double p = data/dt;

            double tupleNumInBatch = 1024*100*1000;
            //double tupleNumInBatch = 51200*750;
            //double tupleNumInBatch = 51200*500;
            double t = tupleNumInBatch/dt;

            System.out.println("latency: "+dt+" s");
            System.out.println("数据量: "+batch.getData().capacity());
            System.out.println("Tuple: "+t+" ge/s");
            // System.out.println("bandWidth: "+p+" MB/s");
            System.exit(0);

        }
      // if(GPU){
        
       // System.out.println("A--"+count);
        // GPU = false;
        Main.processAStep1(batch.getData().array());
        Main.processBStep1(batch.getData().array());
        // Main.processAStep1(batch.getData().array());
        // Main.processBStep1(batch.getData().array());
        // Main.processAStep1(batch.getData().array());
        // Main.processBStep1(batch.getData().array());
        // Main.processAStep1(batch.getData().array());
        // Main.processAStep1(batch.getData().array());
        // Main.processAStep1(batch.getData().array());
        // Main.processAStep1(batch.getData().array());
        //         Main.processAStep1(batch.getData().array());
        // Main.processAStep1(batch.getData().array());
        // GPU = true;
        /* 
        while(GPU==false);
        GPU=false;
        Main.processAStep2();
        GPU=true;
    */
        
      // }
       // else{

      //  System.out.println("B--"+count);
      // Main.processBStep1(batch.getData().array());
/*       
        while(GPU==false);
        GPU=false;
       Main.processBStep2();
        GPU=true;
  */
      // }
    }
}
