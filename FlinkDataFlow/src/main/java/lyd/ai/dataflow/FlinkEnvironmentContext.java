package lyd.ai.dataflow;

import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;



import java.io.IOException;


public class FlinkEnvironmentContext {
    public final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    private final int parallelism = 1;
    private final boolean objectReuse = true;


    public void setUp() throws IOException {
        // set up the execution environment
        env.setParallelism(parallelism);
        env.getConfig().disableSysoutLogging();
        if (objectReuse) {
            env.getConfig().enableObjectReuse();
        }

        env.setStateBackend(new MemoryStateBackend());
    }

    public void execute() throws Exception {
        env.execute();
    }
}
