package transformations;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import tools.Element;
import tools.GlobalConf;

import java.util.Random;

public class MapSampling implements FlatMapFunction<Element, Element> {

	private static final long serialVersionUID = 1L;
	private GlobalConf conf;
	private Random r;
	private double sampleRate;

	public MapSampling(GlobalConf conf) {
		this.conf = conf;
		r = new Random();
	}

	@Override
	public void flatMap(Element value, Collector<Element> output) throws Exception {

		sampleRate = conf.getSampleRate();

		boolean sampled = false;
		if (r.nextDouble() < sampleRate)
			sampled = true;
		if (sampled) {
			output.collect(value);
		}
	}
}