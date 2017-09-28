package transformations;

import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.util.Collector;
import tools.Element;
import tools.Functions;
import tools.GlobalConf;

import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;

public class ReduceComputeRanges extends RichGroupReduceFunction<Element, String> {

	private static final long serialVersionUID = 1L;
	private GlobalConf conf;

	public ReduceComputeRanges(GlobalConf conf) {
		this.conf = conf;
	}

	@Override
	public void reduce(Iterable<Element> input, Collector<String> output)
			throws Exception {

		LinkedList<String> tmpList = new LinkedList<String>();

		Iterator<Element> iterator = input.iterator();
		while (iterator.hasNext()) {
			Element entry = iterator.next();
			tmpList.add(entry.getValue());
		}

		int size = tmpList.size();

		ValueComparator com = new ValueComparator();
		Collections.sort(tmpList, com);

		String q_start = "";
		int len = Functions.maxDecDigits(conf.getDimensions());
		q_start = Functions.createExtra(len);

		// *********************** Estimate ranges ****************************//
		for (int i = 1; i <= conf.getNumOfPartition(); i++) {

			int estRank = Functions.getEstimatorIndex(i, conf.getNoOfElements(),
					conf.getSampleRate(), conf.getNumOfPartition());
			if (estRank - 1 >= size)
				estRank = size;

			String q_end;
			if (i == conf.getNumOfPartition()) {
				q_end = Functions.maxDecString(conf.getDimensions());
			} else
				q_end = tmpList.get(estRank - 1);

			output.collect(q_start + " " + q_end);

			q_start = q_end;
		}
	}

	private class ValueComparator implements Comparator<String> {

		@Override
		public int compare(String w1, String w2) {

			int cmp = w1.compareTo(w2);
			if (cmp != 0)
				return cmp;
			cmp = w1.toString().compareTo(w2.toString());

			return cmp;
		}
	}
}
