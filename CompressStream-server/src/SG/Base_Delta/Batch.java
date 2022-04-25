import java.nio.ByteBuffer;

public class Batch {
    private ByteBuffer data;
    public Batch(int batchSize) {
        data = ByteBuffer.allocate(batchSize);
    }

    public ByteBuffer getData() {
        return data;
    }

}
