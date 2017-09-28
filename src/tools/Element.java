package tools;

import java.io.Serializable;

public class Element implements Serializable {

	private static final long serialVersionUID = 1L;
	private String value;
	private String id;
	private Integer partition;
	private Integer label;

	public Element() {
	}

	public Element(String value, String id, Integer partition, Integer label) {
		this.setValue(value);
		this.setId(id);
		this.setPartition(partition);
		this.setLabel(label);
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public Integer getPartition() {
		return partition;
	}

	public void setPartition(Integer partition) {
		this.partition = partition;
	}

	public Integer getLabel() {
		return label;
	}

	public void setLabel(Integer label) {
		this.label = label;
	}

	@Override
	public String toString() {
		return value + " " + id + " " + partition + " " + label.toString();
	}

}
