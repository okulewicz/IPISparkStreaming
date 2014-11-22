package pl.waw.ipipan.phd.mkopec.sparkReceiver;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.ConnectException;
import java.net.Socket;

import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;

@SuppressWarnings("serial")
public class CustomSocketReceiver extends Receiver<String> {

	private String host;
	private int port;

	public CustomSocketReceiver(String host_, int port_) {
		super(StorageLevel.MEMORY_ONLY());
		host = host_;
		port = port_;
	}

	public void onStart() {
		// Start the thread that receives data over a connection
		new Thread() {
			@Override
			public void run() {
				receive();
			}
		}.start();
	}

	public void onStop() {
		// There is nothing much to do as the thread calling receive()
		// is designed to stop by itself isStopped() returns false
	}

	/** Create a socket connection and receive data until receiver is stopped */
	private void receive() {
		Socket socket = null;
		String userInput = null;

		try {
			// connect to the server
			socket = new Socket(host, port);

			BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));

			// Until stopped or connection broken continue reading
			while (!isStopped() && (userInput = reader.readLine()) != null) {
				System.out.println("Received data '" + userInput + "'");
				store(userInput);
			}
			reader.close();
			socket.close();

			// Restart in an attempt to connect again when server is active
			// again
			restart("Trying to connect again");
		} catch (ConnectException ce) {
			// restart if could not connect to server
			restart("Could not connect", ce);
		} catch (Throwable t) {
			restart("Error receiving data", t);
		}
	}
}