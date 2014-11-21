package pl.waw.ipipan.phd.okulewicz;

import java.util.Random;

public class DataGenerator implements Runnable {

	public static int number = 0; 
	final public static Object lockObj = new Object();
	static Random random = new Random();
	
	public static byte[] getData() {
		return (Integer.toString(number) + "\n").getBytes();
	}

	@Override
	public void run() {
		while (true) {
				++number;
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
	}

}
