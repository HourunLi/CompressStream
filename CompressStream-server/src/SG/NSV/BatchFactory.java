import com.lmax.disruptor.EventFactory;

public class BatchFactory implements EventFactory<Batch>{

    private int batchSize;

    public BatchFactory(int batchSize) {
        this.batchSize = batchSize;
    }


    @Override
    public Batch newInstance() {
        return new Batch(batchSize);
    }
}
