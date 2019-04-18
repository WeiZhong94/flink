/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.api.python;

import py4j.GatewayServer;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * The Py4j Gateway Server provides RPC service for user's python process.
 */
public class PythonShellGatewayServer {

	public static final String DEBUG_FLAG = "PYFLINK_SERVER_DEBUG";

	/**
	 * <p>
	 * Main method to start a local GatewayServer on a ephemeral port.
	 * It tell python side via a handshake socket.
	 *
	 * See: py4j.GatewayServer.main()
	 * </p>
	 */
	public static void main(String[] args) throws IOException, InterruptedException {
		if (args.length == 0) {
			System.exit(1);
		}
		if (System.getenv().containsKey(DEBUG_FLAG)) {
			System.out.println("Debug flag detected, waiting for remote debug connection.");
			Thread.sleep(30 * 1000);
		}

		GatewayServer gatewayServer = new GatewayServer(null, 0);
		gatewayServer.start();

		int serverPort = gatewayServer.getListeningPort();

		// Tell python side the port of our java rpc server
		String handshakeFilePath = args[0];
		File handshakeFile = new File(handshakeFilePath);
		if (handshakeFile.createNewFile()) {
			FileOutputStream fileOutputStream = new FileOutputStream(handshakeFile);
			DataOutputStream stream = new DataOutputStream(fileOutputStream);
			stream.writeInt(serverPort);
			stream.close();
			fileOutputStream.close();
		} else {
			System.out.println("Can't create handshake file: " + handshakeFilePath + ", now exit...");
			return;
		}

		// Exit on EOF or broken pipe.  This ensures that the server dies
		// if its parent program dies.
		while (System.in.read() != -1) {
			// Do nothing
		}
		gatewayServer.shutdown();
	}
}
