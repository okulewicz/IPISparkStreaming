package pl.waw.ipipan.phd.okulewicz;

import java.util.ArrayList;
import java.util.Random;

public class DataGenerator implements Runnable {

	public static long size;
	public static long sleep;
	public static long number = 0;
	public static ArrayList<String> numbers = new ArrayList<String>();
	public static boolean start = false;
	final public static Object lockObj = new Object();
	static Random random = new Random();

	public static byte[] getData() {
		String numbers = "";
		synchronized (lockObj) {
			start = true;
			for (String n : DataGenerator.numbers)
				numbers += n + "\n";
		}
		return (numbers + "\n").getBytes();
	}

	@Override
	public void run() {
		while (true) {
			synchronized (lockObj) {
				if (numbers.size() >= size)
					numbers.remove(0);
				numbers.add(Long.toString(number));
				if (start)
					++number;
			}
			try {
				Thread.sleep(sleep);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}

}
