package lyd.ai.dataflow.functions;

import org.apache.flink.api.common.functions.MapFunction;

public class MultiplyIntLongByTwo implements MapFunction<IntegerLongSource.Record, IntegerLongSource.Record> {
    @Override
    public IntegerLongSource.Record map(IntegerLongSource.Record record) throws Exception {
        return IntegerLongSource.Record.of(record.key, record.value * 2);
    }
}
