package transformations;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import tools.Element;
import tools.GlobalConf;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

public class MapPartition extends RichMapFunction<Element, Element> {

	private static final long serialVersionUID = 1L;
	private String[] rangeArray;
	private GlobalConf conf;
	private Collection<String> ranges;

	public MapPartition(GlobalConf conf) throws IOException {
		this.conf = conf;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		this.ranges = getRuntimeContext().getBroadcastVariable("ranges");
		rangeArray = Arrays.copyOf(this.ranges.toArray(), conf.getNumOfPartition(), String[].class);
	}

	@Override
	public Element map(Element input) throws Exception {

		String pid = getPartitionId(input.getValue());
		if (pid == "") {
			System.out.println("Cannot get pid");
			System.exit(-1);
		}

		int intPid = Integer.valueOf(pid);

		Element element = new Element(input.getValue(), input.getId(), intPid, input.getLabel());
		return element;

	}

	public String getPartitionId(String z) throws IOException {

		String ret = "";
		for (int i = 0; i < conf.getNumOfPartition(); i++) {
			String range = rangeArray[i];
			String[] parts = range.split(" +");
			String low = parts[0];
			String high = parts[1];

			if (z.compareTo(low) >= 0 && z.compareTo(high) <= 0) {
				ret = Integer.toString(i);
			}
		}
		return ret;
	}
}