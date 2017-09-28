package transformations;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import main.CART;
import main.CART.Node;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import tools.GlobalConf;

import java.util.Iterator;

public class ReduceMergeTrees extends RichGroupReduceFunction<Tuple3<Integer, Integer, CART>, String> {

	private static final long serialVersionUID = 1L;
	private GlobalConf conf;

	public ReduceMergeTrees(GlobalConf conf) {
		this.conf = conf;
	}

	@Override
	public void reduce(Iterable<Tuple3<Integer, Integer, CART>> input, Collector<String> output) throws Exception {

		Iterator<Tuple3<Integer, Integer, CART>> iterator = input.iterator();
		Tuple3<Integer, Integer, CART> entry;

		CART[] trees = new CART[conf.getNumOfPartition()];
		while (iterator.hasNext()) {
			entry = iterator.next();
			Integer partition = entry.f1;
			CART tree = entry.f2;
			trees[partition] = tree;
		}

		Node root = trees[0].getRoot();
		for (int i = 0; i < conf.getNumOfPartition() - 1; i++) {
			Node nextRoot = trees[i + 1].getRoot();
			Node falseChild = null;

			if (root.getSplitFeature() == -1) {
				root = nextRoot;
				continue;
			}

			Node tmp = root.getFalseChild();
			if (tmp.getFalseChild() == null) {
				root.setFalseChild(nextRoot);
				continue;
			}

			while (tmp.getFalseChild() != null) {
				falseChild = tmp;
				tmp = falseChild.getFalseChild();
			}
			falseChild.setFalseChild(nextRoot);
		}

		Gson gson = new GsonBuilder().serializeSpecialFloatingPointValues().create();
		String jsonTree = gson.toJson(trees[0]);

		output.collect(jsonTree);

	}
}
