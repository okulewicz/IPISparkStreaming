package example;

import java.io.InputStream;
import java.net.ConnectException;
import java.net.Socket;

import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;

public class JavaCustomReceiver extends Receiver<String> {

	String host = null;
	int port = -1;

	public JavaCustomReceiver(String host_, int port_) {
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

		try {
			// connect to the server
			socket = new Socket(host, port);

			byte[] data = new byte[255];
			InputStream inputStream = socket.getInputStream();
			
			inputStream.read(data);

			store(new String(data).trim());

			socket.close();

			// Restart in an attempt to connect again when server is active
			// again
			restart("Trying to connect again");
		} catch (ConnectException ce) {
			// restart if could not connect to server
			restart("Could not connect", ce);
		} catch (Throwable t) {
			// restart if there is any other error
			restart("Error receiving data", t);
		}
	}
}