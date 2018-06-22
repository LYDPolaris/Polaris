package lyd.ai.dataflow.cep.sources;

import lyd.ai.dataflow.cep.events.MeasureEvent;
import lyd.ai.dataflow.cep.events.MeasureEventType;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Random;

public class ExampleSource extends RichParallelSourceFunction<MeasureEvent> {
	private static final long serialVersionUID = -8217266496516778232L;

	private Random random;
	private boolean running = true;
	
	public ExampleSource() {
		random = new Random();
	}
	
	@Override
	public void run(SourceContext<MeasureEvent> ctx) throws Exception {
		while(running) {
			
			MeasureEvent me;

			switch (random.nextInt(3)) {
			case 0:
				me = new MeasureEvent(getId(), getData(), MeasureEventType.HIGH);
				break;
			case 1:
				me = new MeasureEvent(getId(), getData(), MeasureEventType.NORMAL);
				break;
			case 2:
				me = new MeasureEvent(getId(), getData(), MeasureEventType.LOW);
				break;

			default:
				me = new MeasureEvent(getId(), getData(), MeasureEventType.LOW);
				break;
			}
			
			ctx.collect(me);
			Thread.sleep(250);
		}
	}

	@Override
	public void cancel() {
		running = false;
	}
	
	private long getId() {
		//return 1;
		return Integer.toUnsignedLong(random.nextInt(3)) + 1;
	}
	
	private double getData() {
		return random.nextDouble() * 100;
	}

}
