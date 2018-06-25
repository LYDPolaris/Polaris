package lyd.ai.dataflow.benchmarks.functions;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

public class LongSource extends RichParallelSourceFunction<Long> {

    private volatile boolean running = true;
    private long maxValue;

    public LongSource(long maxValue) {
        this.maxValue = maxValue;
    }

    @Override
    public void run(SourceContext<Long> ctx) throws Exception {
        long counter = 0;

        while (running) {
            synchronized (ctx.getCheckpointLock()) {
                ctx.collect(counter);
                counter++;
                if (counter >= maxValue) {
                    cancel();
                }
            }
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
