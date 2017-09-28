package tools;

import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;

import java.io.*;
import java.math.BigInteger;
import java.util.LinkedList;
import java.util.concurrent.ThreadLocalRandom;

/**
 * This class contains various functions that
 * are required throughout the code.
 */
public class Functions {

	public static void generateDatasets(GlobalConf conf) throws IOException {

		String line;

		//String datasetPath = conf.getDataPath() + "dataset/demographics";
		//String datasetPath = conf.getDataPath() + "dataset/abalone_data";
		//String datasetPath = conf.getDataPath() + "dataset/seeds_dataset";
		//String datasetPath = conf.getDataPath() + "dataset/mammographic_masses";
		String datasetPath = conf.getDataPath() + "dataset/magic04";
		Path pt = new Path(datasetPath);
		FileSystem fs = pt.getFileSystem();
		BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));

		pt = new Path(conf.getDataPath() + "dataset/training");
		fs = pt.getFileSystem();
		fs.delete(pt, true);
		BufferedWriter bw1 = new BufferedWriter(new OutputStreamWriter(fs.create(pt, true)));

		pt = new Path(conf.getDataPath() + "dataset/testing");
		fs = pt.getFileSystem();
		fs.delete(pt, true);
		BufferedWriter bw2 = new BufferedWriter(new OutputStreamWriter(fs.create(pt, true)));

		LinkedList<String> lines = new LinkedList<>();
		while ((line = br.readLine()) != null) {
			lines.add(line);
		}

		int[] indices = new int[lines.size()];
		for (int i = 0; i < lines.size(); i++) {
			indices[i] = ThreadLocalRandom.current().nextInt(0, lines.size());
		}

		for (int i = 0; i < Math.round(0.8 * indices.length); i++) {
			bw1.write(lines.get(indices[i]) + "\n");
		}

		for (int i = (int) (Math.round(0.8 * indices.length) + 1); i < indices.length; i++) {
			bw2.write(lines.get(indices[i]) + "\n");
		}

		bw1.close();
		bw2.close();
		br.close();
	}

	/**
	 * source: http://www.cs.utah.edu/~lifeifei/knnj/#codes
	 *
	 * @param num
	 * @return
	 */
	public static String createExtra(int num) {
		if (num < 1)
			return "";

		char[] extra = new char[num];
		for (int i = 0; i < num; i++)
			extra[i] = '0';
		return (new String(extra));
	}

	/**
	 * @param dimension
	 * @return
	 */
	public static int maxDecDigits(int dimension) {
		int max = 32;
		BigInteger maxDec = new BigInteger("1");
		maxDec = maxDec.shiftLeft(dimension * max);
		maxDec.subtract(BigInteger.ONE);
		return maxDec.toString().length();
	}

	/**
	 * @param dimension
	 * @return
	 */
	public static String maxDecString(int dimension) {
		int max = 32;
		BigInteger maxDec = new BigInteger("1");
		maxDec = maxDec.shiftLeft(dimension * max);
		maxDec.subtract(BigInteger.ONE);
		return maxDec.toString();
	}

	/**
	 * source: http://www.cs.utah.edu/~lifeifei/knnj/#codes
	 *
	 * @param i
	 * @param size
	 * @param sampleRate
	 * @param numOfPartition
	 * @return
	 */
	public static int getEstimatorIndex(int i, int size, double sampleRate,
										int numOfPartition) {
		double iquantile = (i * 1.0 / numOfPartition);
		int orgRank = (int) Math.ceil((iquantile * size));
		int estRank = 0;

		int val1 = (int) Math.floor(orgRank * sampleRate);
		int val2 = (int) Math.ceil(orgRank * sampleRate);

		int est1 = (int) (val1 * (1 / sampleRate));
		int est2 = (int) (val2 * (1 / sampleRate));

		int dist1 = Math.abs(est1 - orgRank);
		int dist2 = Math.abs(est2 - orgRank);

		if (dist1 < dist2)
			estRank = val1;
		else
			estRank = val2;

		return estRank;
	}

	public static double calculateAccuracy(LinkedList<Integer> predictions, LinkedList<Integer> labels) {

		int total_correct = 0;
		for (int i = 0; i < labels.size(); i++) {
			if (predictions.get(i) == labels.get(i))
				total_correct++;
		}

		return 100.0 * total_correct / labels.size();
	}

}