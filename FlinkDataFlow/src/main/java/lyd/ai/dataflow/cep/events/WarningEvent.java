package lyd.ai.dataflow.cep.events;

public class WarningEvent extends MeasureEvent {
	public WarningEvent() {
	}

	public WarningEvent(long id, double data, MeasureEventType met) {
		super(id, data, met);
	}

	@Override
	public String toString() {
		return "WarningEvent [getId()=" + getId() + ", getData()=" + getData() + ", getEventTime()=" + getEventTime()
				+ ", getEventType()=" + getEventType() + "]";
	}

}
