package pl.waw.ipipan.phd.okulewicz;

import java.util.Random;

public class DataGenerator implements Runnable {

	public static long number = 0; 
	final public static Object lockObj = new Object();
	static Random random = new Random();
	
	public static byte[] getData() {
		long number = 0;
		synchronized(lockObj) {
			number = DataGenerator.number;
		}
		return (Long.toString(number) + "\n").getBytes();
	}

	@Override
	public void run() {
		while (true) {
			synchronized (lockObj) {
				++number;
			}
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
	}

}
