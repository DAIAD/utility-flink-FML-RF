package transformations;

import org.apache.flink.api.common.functions.RichMapFunction;
import tools.Element;
import tools.GlobalConf;
import tools.Zorder;

public class MapTransform extends RichMapFunction<String, Element> {

	private static final long serialVersionUID = 1L;
	private GlobalConf conf;

	public MapTransform(GlobalConf conf) {
		this.conf = conf;
	}

	@Override
	public Element map(String input) throws Exception {

		String val = null;
		String line = input;
		char ch = ';';
		int pos = line.indexOf(ch);
		String id = line.substring(0, pos);
		String rest = line.substring(pos + 1, line.length()).trim();
		String[] parts = rest.split(";");
		float[] coord = new float[conf.getDimensions()];
		for (int i = 0; i < conf.getDimensions(); i++) {
			try {
				coord[i] = Float.valueOf(parts[i]);
			} catch (NumberFormatException e) {
				continue;
			}
		}

		int[] converted_coord = new int[conf.getDimensions()];
		for (int k = 0; k < conf.getDimensions(); k++) {
			coord[k] = coord[k] * conf.getScale()[k];
			converted_coord[k] = (int) (coord[k] * 10);
		}

		val = Zorder.valueOf(conf.getDimensions(), converted_coord);
		return new Element(val, id, 0, Integer.valueOf(parts[conf.getDimensions()]));

	}
}