package org.apache.flink.api.python;

import py4j.GatewayServer;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;

/**
 * The Py4j Gateway Server provides RPC service for user's python process.
 */
public class PythonShellGatewayServer {

	/**
	 * <p>
	 * Main method to start a local GatewayServer on a ephemeral port.
	 * It tell python side via a handshake socket.
	 *
	 * See: py4j.GatewayServer.main()
	 * </p>
	 */
	public static void main(String[] args) throws IOException {
		if (args.length == 0) {
			System.exit(1);
		}

		GatewayServer gatewayServer = new GatewayServer(null, 0);
		gatewayServer.start();

		int serverPort = gatewayServer.getListeningPort();

		// Tell python side the port of our java rpc server
		int handshakePort = Integer.valueOf(args[0]);
		String localhost = "127.0.0.1";
		Socket handshakeSocket = new Socket(localhost, handshakePort);
		DataOutputStream dos = new DataOutputStream(handshakeSocket.getOutputStream());
		dos.writeInt(serverPort);
		dos.flush();
		dos.close();
		handshakeSocket.close();

		// Exit on EOF or broken pipe.  This ensures that the server dies
		// if its parent program dies.
		while (System.in.read() != -1) {
			// Do nothing
		}
		System.out.println("Existing...");
	}
}
