package fmlrf;

import main.CART;
import main.CART.SplitRule;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.core.fs.Path;
import tools.Element;
import tools.Functions;
import tools.GlobalConf;
import transformations.*;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.LineNumberReader;

public class FML_RF {

	private SplitRule rule;

	public FML_RF(SplitRule rule) {
		this.rule = rule;
	}

	public String execute(GlobalConf conf) throws Exception {

		Functions.generateDatasets(conf);

		// Count the lines of the dataset
		LineNumberReader lnr;
		String datasetPath = conf.getDataPath() + "dataset/training";
		Path pt = new Path(datasetPath);
		FileSystem fs = pt.getFileSystem();
		BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));

		lnr = new LineNumberReader(br);
		lnr.skip(Long.MAX_VALUE);
		conf.setNoOfElements(lnr.getLineNumber());
		lnr.close();

		//*************************** Sampling setting ****************************//
		conf.setSampleRate(1 / (conf.getEpsilon() * conf.getEpsilon() * conf.getNoOfElements()));

		if (conf.getSampleRate() > 1) conf.setSampleRate(1);

		if (conf.getSampleRate() * conf.getNoOfElements() < 1) {
			System.out.printf("Increase sampling rate of R :  " + conf.getSampleRate());
			System.exit(-1);
		}

		// Set up the execution environment
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		//////////////*********** FLINK EXECUTE **************///////////////////
		/******** STAGE 1 ***********/
		DataSet<String> dataset = env.readTextFile(datasetPath);

		//########## MAP
		DataSet<Element> transformedDataset = dataset.map(new MapTransform(conf));

		/////////// SAMPLING MAP
		DataSet<Element> sampledDataset = transformedDataset.flatMap(new MapSampling(conf));

		//########## REDUCE
		DataSet<String> ranges = sampledDataset.groupBy("partition").reduceGroup(new ReduceComputeRanges(conf));
		ranges.writeAsText(conf.getDataPath() + "results/Ranges", WriteMode.OVERWRITE).setParallelism(1);

		/******** STAGE 2 ***********/
		//########## MAP
		DataSet<Element> result = transformedDataset.map(new MapPartition(conf)).withBroadcastSet(ranges, "ranges");
		DataSet<Element> partitionedData = result.partitionCustom(new MyPartitioner(), "partition").sortPartition("value", Order
				.ASCENDING);
		partitionedData.writeAsText(conf.getDataPath() + "results/PartitionedData", WriteMode.OVERWRITE).setParallelism(1);

		//########## REDUCE
		DataSet<Tuple3<Integer, Integer, CART>> trees = partitionedData.groupBy("partition").reduceGroup(new ReduceDecisionTrees(conf,
				rule));
		trees.writeAsText(conf.getDataPath() + "results/trees", WriteMode.OVERWRITE);

		/******** STAGE 3 ***********/
		//########## REDUCE AND WRITE RESULT
		DataSet<String> randomForest = trees.groupBy(0).reduceGroup(new ReduceMergeTrees(conf));
		randomForest.writeAsText(conf.getDataPath() + "randomForest", WriteMode.OVERWRITE).setParallelism(1);

		// execute program
		long startExecuteTime = System.currentTimeMillis();
		env.execute("FML-kNN");
		long totalElapsedExecuteTime = System.currentTimeMillis() - startExecuteTime;

		// Count execution time
		int ExecuteMillis = (int) totalElapsedExecuteTime % 1000;
		int ExecuteSeconds = (int) (totalElapsedExecuteTime / 1000) % 60;
		int ExecuteMinutes = (int) ((totalElapsedExecuteTime / (1000 * 60)) % 60);
		int ExecuteHours = (int) ((totalElapsedExecuteTime / (1000 * 60 * 60)) % 24);
		return "Total time: " + ExecuteHours + "h " + ExecuteMinutes
				+ "m " + ExecuteSeconds + "sec " + ExecuteMillis + "mil";

	}

	private static class MyPartitioner implements Partitioner<Integer> {

		private static final long serialVersionUID = 1L;

		@Override
		public int partition(Integer key, int numPartitions) {
			return key % numPartitions;
		}
	}
}