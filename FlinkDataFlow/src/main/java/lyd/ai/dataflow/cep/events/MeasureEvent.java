package lyd.ai.dataflow.cep.events;

public class MeasureEvent {
	
	private long id;
	private double data;
	private long eventTime;
	private MeasureEventType eventType;

	public MeasureEvent() {
	}
	
	public MeasureEvent(long id, double data, MeasureEventType met) {
		this.id = id;
		this.data = data;
		this.eventType = met;
		this.eventTime = System.currentTimeMillis();
	}

	public long getId() {
		return id;
	}

	public void setId(long id) {
		this.id = id;
	}

	public double getData() {
		return data;
	}

	public void setData(double data) {
		this.data = data;
	}

	public long getEventTime() {
		return eventTime;
	}

	public void setEventTime(long eventTime) {
		this.eventTime = eventTime;
	}

	public MeasureEventType getEventType() {
		return eventType;
	}

	public void setEventType(MeasureEventType eventType) {
		this.eventType = eventType;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		long temp;
		temp = Double.doubleToLongBits(data);
		result = prime * result + (int) (temp ^ (temp >>> 32));
		result = prime * result + (int) (eventTime ^ (eventTime >>> 32));
		result = prime * result + ((eventType == null) ? 0 : eventType.hashCode());
		result = prime * result + (int) (id ^ (id >>> 32));
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		MeasureEvent other = (MeasureEvent) obj;
		if (Double.doubleToLongBits(data) != Double.doubleToLongBits(other.data))
			return false;
		if (eventTime != other.eventTime)
			return false;
		if (eventType != other.eventType)
			return false;
		if (id != other.id)
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "MeasureEvent [id=" + id + ", data=" + data + ", eventTime=" + eventTime + ", eventType=" + eventType
				+ "]";
	}
}
