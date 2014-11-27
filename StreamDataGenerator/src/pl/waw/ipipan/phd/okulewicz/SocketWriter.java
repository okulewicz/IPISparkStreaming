package pl.waw.ipipan.phd.okulewicz;

import java.io.IOException;
import java.net.Socket;
import java.util.Random;

class SocketWriter implements Runnable {

	static Random random = new Random();
	public Socket socket;
	static long c = 0L;
	static long sleep = 100L;

	@Override
	public void run() {
		// TODO Auto-generated method stub
		while (true) {
			byte[] data = DataGenerator.getData();
			for (int i = 0; i < data.length; ++i)
				System.out.print((char) data[i]);
			try {
				System.out.println("C=" + inc());
				Thread.sleep(sleep);
				socket.getOutputStream().write(data);
				// socket.close();
			} catch (IOException | InterruptedException e) {
				// TODO Auto-generated catch block
				// e.printStackTrace();

				try {
					socket.close();
				} catch (IOException e1) {
					// TODO Auto-generated catch block
					// e1.printStackTrace();
				}
				break;
			}
		}
	}

	synchronized static long inc() {
		return c++;
	}

}
