package lyd.ai.dataflow.exercises.datastream.utils;

import lyd.ai.dataflow.exercises.datastream.datatypes.ConnectedCarEvent;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

public class ConnectedCarAssigner implements AssignerWithPunctuatedWatermarks<ConnectedCarEvent> {
	@Override
	public long extractTimestamp(ConnectedCarEvent event, long previousElementTimestamp) {
		return event.timestamp;
	}

	@Override
	public Watermark checkAndGetNextWatermark(ConnectedCarEvent event, long extractedTimestamp) {
		// simply emit a watermark with every event
		return new Watermark(extractedTimestamp - 30000);
	}
}
