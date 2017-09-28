package tools;

import java.math.BigInteger;
import java.util.Vector;

/**
 * source: http://www.cs.utah.edu/~lifeifei/knnj/#codes
 **/
public class Zorder {

	/**
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
	 * Convert an multi-dimensional coordinate into a zorder
	 * coordinates have already been scaled and shifted
	 *
	 * @param dimension
	 * @param coord
	 * @return
	 */
	public static String valueOf(int dimension, int[] coord) {
		Vector<String> arrPtr = new Vector<String>(dimension);
		// System.out.println( "maxDec " + maxDec.toString() );
		int max = 32;
		int fix = Functions.maxDecDigits(dimension); // global maximum possible zvalue
		// length
		// System.out.println( fix );

		for (int i = 0; i < dimension; i++) {
			String p = Integer.toBinaryString((int) coord[i]);
			// System.out.println( coord[i] + " " + p );
			arrPtr.add(p);
		}

		for (int i = 0; i < arrPtr.size(); ++i) {
			String extra = createExtra(max - arrPtr.elementAt(i).length());
			arrPtr.set(i, extra + arrPtr.elementAt(i));
			// System.out.println( i + " " + arrPtr.elementAt(i) );
		}

		char[] value = new char[dimension * max];
		int index = 0;

		// Create Zorder
		for (int i = 0; i < max; ++i) {
			for (String e : arrPtr) {
				char ch = e.charAt(i);
				value[index++] = ch;
			}
		}

		String order = new String(value);
		// System.out.println( value );
		// Covert a binary representation of order into a big integer
		BigInteger ret = new BigInteger(order, 2);

		// Return a fixed length decimal String representation of
		// the big integer (z-order)
		order = ret.toString();
		// System.out.println( order );
		if (order.length() < fix) {
			String extra = createExtra(fix - order.length());
			order = extra + order;
		} else if (order.length() > fix) {
			System.out.println("too big zorder, need to fix Zorder.java");
			System.exit(-1);
		}

		//nt[] tmp = toCoord(order, 12);

		return order;
	}

	/**
	 * @param z
	 * @param dimension
	 * @return
	 */
	public static int[] toCoord(String z, int dimension) {
		int DECIMAL_RADIX = 10;
		int BINARY_RADIX = 2;

		if (z == null) {
			System.out.println("Z-order Null pointer!!!@Zorder.toCoord");
			System.exit(-1);
		}

		BigInteger bigZ = new BigInteger(z, DECIMAL_RADIX);
		String bigZStr = bigZ.toString(BINARY_RADIX);

		// Test
		// bigZStr = "1110011";
		// System.out.println(bigZStr);

		int len = bigZStr.length();
		// System.out.println("leng before is" + len);
		// int prefixZeros = len % dimension;
		int prefixZeros = 0;
		if (len % dimension != 0)
			prefixZeros = dimension - len % dimension;

		// System.out.println("--");
		// System.out.println(prefixZeros);

		String prefix = Zorder.createExtra(prefixZeros);
		bigZStr = prefix + bigZStr;

		len = bigZStr.length();
		// System.out.println(len);

		if (len % dimension != 0) {
			System.out.println("Wrong prefix!!!@Zorder.toCoord");
			System.exit(-1);
		}

		// The most significant bit is save at starting position of
		// the char array.
		char[] bigZCharArray = bigZStr.toCharArray();

		int[] coord = new int[dimension];
		for (int i = 0; i < dimension; i++)
			coord[i] = 0;
		for (int i = 0; i < bigZCharArray.length; ) {
			for (int j = 0; j < dimension; ++j) {
				coord[j] <<= 1;
				coord[j] |= bigZCharArray[i++] - '0';
			}
		}

		return coord;
	}
}