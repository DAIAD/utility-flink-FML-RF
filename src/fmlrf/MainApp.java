package fmlrf;

import com.google.gson.Gson;
import main.CART;
import tools.Functions;
import tools.GlobalConf;
import tools.Zorder;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Hashtable;
import java.util.LinkedList;

public class MainApp {

	public static void main(String[] args) throws Exception {

		GlobalConf conf = new GlobalConf();

		if (args.length > 0) {
			try {
				//conf.setNumIterations(Integer.parseInt(args[0]));
			} catch (Exception e) {
				System.out.println("Error! Please check arguments.");
				System.exit(0);
			}
		} else {
			System.out.println("Executing FML-RF with default parameters.");
		}

		// Generate scale
		// TODO: This will be generated automatically using cross-validation
		int[] scale = new int[conf.getDimensions()];
		for (int i = 0; i < scale.length; i++) {
			scale[i] = 1;
		}
		conf.setScale(scale);

		int numberOfRuns = 100;
		double totalAccuracy = 0.0;
		for (int i = 0; i < numberOfRuns; i++) {
			FML_RF randomForest = new FML_RF(CART.SplitRule.ENTROPY);
			System.out.println(randomForest.execute(conf));
			totalAccuracy += runTest(conf);
		}
		System.out.println("Final accuracy: " + totalAccuracy / numberOfRuns + "%");
	}

	public static double runTest(GlobalConf conf) throws NumberFormatException, IOException {
		LinkedList<double[]> testSet = new LinkedList<>();
		LinkedList<Integer> labels = new LinkedList<>();
		BufferedReader br = new BufferedReader(new FileReader(conf.getDataPath() + "dataset/testing"));
		String line = "";

		while ((line = br.readLine()) != null) {
			String[] parts = line.split(";");
			double[] data = new double[parts.length - 2];
			for (int i = 1; i < parts.length - 1; i++) {
				data[i - 1] = Double.parseDouble(parts[i]);
			}
			testSet.add(data);
			labels.add(Integer.parseInt(parts[parts.length - 1]));
		}
		br.close();

		Gson gson = new Gson();
		LinkedList<Integer> predictions = new LinkedList<>();

		// Read the forest from disk
		CART[] forest = new CART[conf.getNumOfTrees()];
		br = new BufferedReader(new FileReader(conf.getDataPath() + "randomForest"));
		int count = 0;
		while ((line = br.readLine()) != null) {
			forest[count++] = gson.fromJson(line, CART.class);
		}

		for (int i = 0; i < testSet.size(); i++) {
			double[] element = testSet.get(i);
			int[] converted_coord = new int[element.length];
			for (int k = 0; k < element.length; k++) {
				element[k] = element[k] * conf.getScale()[k];
				converted_coord[k] = (int) (element[k] * 10);
			}

			String val = Zorder.valueOf(conf.getDimensions(), converted_coord);
			element = new double[1];
			element[0] = Double.parseDouble(val);

			Hashtable<Integer, Integer> votes = new Hashtable<Integer, Integer>() {
				private static final long serialVersionUID = 1L;

				{
					for (int i = 0; i < conf.getNumOfClasses(); i++)
						put(i + 1, 0);
				}
			};

			for (int j = 0; j < forest.length; j++) {
				int result = forest[j].predict(element) + 1;
				int vote = votes.get(result);
				votes.put(result, ++vote);
			}

			Integer maxKey = null;
			Integer maxValue = Integer.MIN_VALUE;
			for (Integer entry : votes.keySet()) {
				if (votes.get(entry) > maxValue) {
					maxValue = votes.get(entry);
					maxKey = entry;
				}
			}

			predictions.add(maxKey);
		}

		return Functions.calculateAccuracy(predictions, labels);
	}
}