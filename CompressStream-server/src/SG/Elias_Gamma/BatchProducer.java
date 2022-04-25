import com.lmax.disruptor.RingBuffer;

import java.nio.ByteBuffer;

public class BatchProducer {
    private final RingBuffer<Batch> ringbuffer;
    public BatchProducer(RingBuffer<Batch> ringbuffer) {
        this.ringbuffer = ringbuffer;
    }

    public void produceData(ByteBuffer data) {
        System.out.println(data);
        long sequence = ringbuffer.next();
        Batch batch = ringbuffer.get(sequence);
        //setData

        data.clear();
        batch.getData().clear();
        batch.getData().put(data);
        data.clear();
        batch.getData().rewind();

        ringbuffer.publish(sequence);
    }
}
