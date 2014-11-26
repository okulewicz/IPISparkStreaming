package pl.waw.ipipan.phd.okulewicz;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public class StreamDataGenerator {

	static boolean stop = false;
	static ServerSocket listenSocket;
	
	public static void main(String[] args) {
		if (args.length < 2) {
			System.out.println("Usage: " + StreamDataGenerator.class.getSimpleName()
					+ " <size> <sleep_time_milis>");
			return;
		}

		try {
			DataGenerator.size = Long.parseLong(args[0]);
			DataGenerator.sleep = Long.parseLong(args[1]);
		} catch (NumberFormatException ex) {
			System.err.println("Error parsing program arguments: " + ex);
			return;
		}

		Runtime.getRuntime().addShutdownHook(new Thread()
        {
            @Override
            public void run()
            {
                stop = true;
                System.out.println("Shutdown hook ran!");
            }
        });
		
		try {
			listenSocket = new ServerSocket(12345);
			new Thread(new DataGenerator()).start();
			while (!stop) {
				Socket socket = listenSocket.accept();
				SocketWriter sw = new SocketWriter();
				sw.socket = socket;
				Thread t = new Thread(sw);
				t.start();
			}
			listenSocket.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
