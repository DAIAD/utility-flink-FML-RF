package transformations;

import main.CART;
import main.CART.SplitRule;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import tools.Element;
import tools.GlobalConf;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.ThreadLocalRandom;

public class ReduceDecisionTrees extends RichGroupReduceFunction<Element, Tuple3<Integer, Integer, CART>> {

	private static final long serialVersionUID = 1L;
	private GlobalConf conf;
	private SplitRule rule;

	public ReduceDecisionTrees(GlobalConf conf, SplitRule rule) {
		this.conf = conf;
		this.rule = rule;
	}

	@Override
	public void reduce(Iterable<Element> input, Collector<Tuple3<Integer, Integer, CART>> output) throws Exception {

		Iterator<Element> iterator = input.iterator();
		Element entry = null;

		ArrayList<double[]> instances = new ArrayList<double[]>();
		while (iterator.hasNext()) {
			entry = iterator.next();
			String value = entry.getValue();
			int label = entry.getLabel();
			double[] element = new double[2];
			element[0] = Double.parseDouble(value);
			element[1] = label - 1;
			instances.add(element);
		}


		double[][] trainInstances;
		int[] trainLabels;
		for (int i = 0; i < conf.getNumOfTrees(); i++) {
			trainInstances = new double[instances.size()][1];
			trainLabels = new int[instances.size()];

			for (int j = 0; j < instances.size(); j++) {
				int index = ThreadLocalRandom.current().nextInt(0, instances.size());
				trainInstances[j][0] = instances.get(index)[0];
				trainLabels[j] = (int) instances.get(index)[1];
			}

			CART tree = new CART(trainInstances, trainLabels, conf.getMaxNodes(), conf.getNodeSize(), rule, conf.getNumOfClasses());
			Integer partition = entry.getPartition();

			output.collect(new Tuple3<Integer, Integer, CART>(i, partition, tree));
		}
	}
}
