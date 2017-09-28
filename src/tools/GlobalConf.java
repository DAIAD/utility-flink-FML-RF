package tools;

import java.io.Serializable;

public class GlobalConf implements Serializable {

	private static final long serialVersionUID = 1L;
	private String dataPath = "";
	private int noOfElements;
	private double epsilon = 0.003;
	private double sampleRate;
	private int[] scale;
	private int numOfPartition = 16;

	// Tree related settings
	private int numOfTrees = 300;
	private int maxNodes = 17;
	private int nodeSize = 2;

	// Dataset related settings
	private int dimensions = 10;
	private int numOfClasses = 2;

	public String getDataPath() {
		return dataPath;
	}

	public void setDataPath(String dataPath) {
		this.dataPath = dataPath;
	}

	public int getNoOfElements() {
		return noOfElements;
	}

	public void setNoOfElements(int noOfElements) {
		this.noOfElements = noOfElements;
	}

	public double getEpsilon() {
		return epsilon;
	}

	public void setEpsilon(double epsilon) {
		this.epsilon = epsilon;
	}

	public double getSampleRate() {
		return sampleRate;
	}

	public void setSampleRate(double sampleRate) {
		this.sampleRate = sampleRate;
	}

	public int getDimensions() {
		return dimensions;
	}

	public void setDimensions(int dimensions) {
		this.dimensions = dimensions;
	}

	public int[] getScale() {
		return scale;
	}

	public void setScale(int[] scale) {
		this.scale = scale;
	}

	public int getNumOfPartition() {
		return numOfPartition;
	}

	public void setNumOfPartition(int numOfPartition) {
		this.numOfPartition = numOfPartition;
	}

	public int getNumOfClasses() {
		return numOfClasses;
	}

	public void setNumOfClasses(int numOfClasses) {
		this.numOfClasses = numOfClasses;
	}

	public int getNumOfTrees() {
		return numOfTrees;
	}

	public void setNumOfTrees(int numOfTrees) {
		this.numOfTrees = numOfTrees;
	}

	public int getMaxNodes() {
		return maxNodes;
	}

	public void setMaxNodes(int maxNodes) {
		this.maxNodes = maxNodes;
	}

	public int getNodeSize() {
		return nodeSize;
	}

	public void setNodeSize(int nodeSize) {
		this.nodeSize = nodeSize;
	}
}