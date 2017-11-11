import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;

public class BranchServer {

	static int branchBalance=0;
	int branchInitialBalance=0;	
	static String branchIPAddress;
	static String branchName;
	static int branchPort;
	Bank.BranchMessage allBranchDetails;
	static Map<String, Socket> map = new HashMap<>();
	static int establishedTcpConnections = 0;

	//Flag to differentiate Branch messages and simple string messages
	static int flag=0;
	
	private final Object lock = new Object();
	
	private ConcurrentHashMap<Integer, Integer> branchState = new ConcurrentHashMap<>();
	private ConcurrentHashMap<Integer, Map<String, Integer>> incomingChannelStates = new ConcurrentHashMap<>();
	private ConcurrentHashMap<Integer, Map<String, Integer>> finalChannelStates = new ConcurrentHashMap<>();
	
	@SuppressWarnings("resource")
	public static void main(String[] args) {
		if(args.length != 2){
			System.out.println("please provide a valid branch-name and port");
			System.exit(0);
		}
		ServerSocket serverSocket = null;
		BranchServer branchServer = new BranchServer();
		
		try {
				branchIPAddress = InetAddress.getLocalHost().getHostAddress();
				branchName = args[0];
				branchPort = Integer.valueOf(args[1]);
				
				serverSocket = new ServerSocket(branchPort);
				
				System.out.println("Branch server started " + branchIPAddress + " " + branchPort);
				
				Bank.BranchMessage branchMsg = null;
				
				Socket socket = serverSocket.accept();
				
				
				InputStream inputStream = socket.getInputStream();
				
				//Init branch message from controller
				branchMsg = Bank.BranchMessage.parseDelimitedFrom(inputStream);
				if(branchMsg.hasInitBranch()) {
					branchServer.initializeBranchDetails(branchMsg);
					branchServer.setUpTCPConnections();
				}
				
				new ControllerHandler(socket, branchServer).start();
				
			}catch (IOException e) {
				e.printStackTrace();
			}
		
			while(true) {
				try {
						Socket socket = serverSocket.accept();
						//System.out.println("connection accepted");
						if(flag == 0) {
							BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
							String name = in.readLine();
							
							map.put(name, socket);
							establishedTcpConnections+=1;
							
							//Start listening for messages and then start money transfer
							new BranchHandler(socket,branchServer, name).start();
							branchServer.syncTCPConnectionsAndStartMoneyTransfer();	
						}/*else {
							Bank.BranchMessage msg = Bank.BranchMessage.parseDelimitedFrom(socket.getInputStream());
							
							if(msg.hasInitSnapshot()){
								System.out.println("------\nReceived snapshot message from controller:::" + msg.getInitSnapshot().getSnapshotId() +"--------");
								new ControllerHandler(socket, branchServer).start();
								//branchServer.initiateLocalSnapshotProcedure(msg.getInitSnapshot().getSnapshotId());
							}
							
							if(msg.hasRetrieveSnapshot()) {
								branchServer.returnSnapshotToController(msg.getRetrieveSnapshot().getSnapshotId());
							}
						}*/
						
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
	}
	
	/**
	 * Set up TCP connections with the Branches after itself in the branch list
	 */
	private void setUpTCPConnections() {
		int i=0;
		for(i=0; i<allBranchDetails.getInitBranch().getAllBranchesCount(); i++) {
			if(allBranchDetails.getInitBranch().getAllBranches(i).getName().equals(branchName)) {
				i++;
				break;
			}
		}
		Socket socket = null;
		
		for(int j=i; j<allBranchDetails.getInitBranch().getAllBranchesCount(); j++) {
				String ipAddr = allBranchDetails.getInitBranch().getAllBranches(j).getIp();
				int port = allBranchDetails.getInitBranch().getAllBranches(j).getPort();
				String name = allBranchDetails.getInitBranch().getAllBranches(j).getName();
				
				try {
					socket = new Socket(ipAddr, port);
					
					PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
					//Send your identity to the connecting branch
					out.println(branchName);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				map.put(name, socket);
				establishedTcpConnections+=1;
				
				//Start listening for messages and then start money transfer
				new BranchHandler(socket, this, name).start();
				syncTCPConnectionsAndStartMoneyTransfer();
		}
		
	}
	
	private static class ControllerHandler extends Thread {
        private Socket cSocket;
        private BranchServer branchServer;
        
        public ControllerHandler(Socket socket, BranchServer server) {
        	cSocket = socket;
        	branchServer = server;
        }
 
        public void run() {
        	try {
        		InputStream inputStream = cSocket.getInputStream();
        		Bank.BranchMessage.Builder branchMsgBuilder  = Bank.BranchMessage.newBuilder();
        		
        		Bank.BranchMessage branchMessage = null;
        		while( (branchMessage = Bank.BranchMessage.parseDelimitedFrom(inputStream)) != null){
        			
        			if(branchMessage.hasInitSnapshot()) {
        				branchServer.initiateLocalSnapshotProcedure(branchMessage.getInitSnapshot().getSnapshotId());
        			}
        			
        			if(branchMessage.hasRetrieveSnapshot()) {
        				Bank.ReturnSnapshot returnSnapshot = branchServer.returnSnapshotToController(branchMessage.getRetrieveSnapshot().getSnapshotId());
        				
        				branchMsgBuilder  = Bank.BranchMessage.newBuilder();
        				branchMsgBuilder.setReturnSnapshot(returnSnapshot);
        				
        				branchMsgBuilder.build().writeDelimitedTo(cSocket.getOutputStream());
        			}
        		}
        		
			} catch (IOException e) {
				e.printStackTrace();
			}
			
        }
    }
	
	private static class BranchHandler extends Thread {
        private Socket clientSocket;
        private BranchServer branchServer;
        private String fromBranch;
        
        public BranchHandler(Socket socket, BranchServer server, String name) {
            clientSocket = socket;
            branchServer = server;
            fromBranch = name;
        }
 
        public void run() {
        	try {
        		InputStream inputStream = clientSocket.getInputStream();
        		
        		Bank.BranchMessage branchMessage = null;
        		while( (branchMessage = Bank.BranchMessage.parseDelimitedFrom(inputStream)) != null){
        			if(branchMessage.hasTransfer()) {
						int amount = branchMessage.getTransfer().getMoney();
						branchServer.updateBalance(amount);
						branchServer.updateAmountForAllRecordingChannels(amount, fromBranch);
					}
        			
        			if(branchMessage.hasMarker()) {
        				branchServer.receiveMarkerMessage(branchMessage.getMarker().getSnapshotId(), fromBranch);
        			}
        		}
			} catch (IOException e) {
				e.printStackTrace();
			}
			
        }
    }
    
	/**
	 * This method starts money transfer only after it has established all the tcp connections with remaining all branches
	 */
	public void syncTCPConnectionsAndStartMoneyTransfer() {
		int expectedConnections = allBranchDetails.getInitBranch().getAllBranchesCount()-1;
		
		if(expectedConnections == establishedTcpConnections) {
			System.out.println("Yeah!!!All connections established, Starting transactions");
			flag = 1;
			
			transferMoneyToAllBranches();
		}
		
	}
	
	/**
	 * This function is used to initiaite snapshot for local branch
	 * Step 1: Record its own local state (Mark as 1st marker received)
	 * step 2: Send marker Messages to other branches
	 * step 3: start recording on incoming channel
	 */
	private void initiateLocalSnapshotProcedure(int snapshotId) {
		
		//Record local state
		recordLocalState(snapshotId);
		
		//start recording for other channels
		createMapForIncomingChannels(snapshotId);

		//send marker to other branches
		Bank.Marker.Builder markerMsg = Bank.Marker.newBuilder();
		markerMsg.setSnapshotId(snapshotId);
		sendMarkerMessagesToOtherBranches(markerMsg);

	}
	
	/**
	 * This function sends marker messages to all branches except itself
	 * @param marker
	 */
	private void sendMarkerMessagesToOtherBranches(Bank.Marker.Builder marker) {
		Bank.BranchMessage.Builder branchMesssageBuilder  = Bank.BranchMessage.newBuilder();
		branchMesssageBuilder.setMarker(marker);
		
		//Map<String, Integer> recordingMap = incomingChannelStates.get(snapshotId);
		Iterator it = map.entrySet().iterator();
		
	    while (it.hasNext()) {
	        Map.Entry pair = (Map.Entry)it.next();
	        	System.out.println();
	        	//System.out.println("----Sending marker message to----- " + pair.getKey());
	        	Socket soc = map.get(pair.getKey());
	        	
	        	OutputStream outputStream;
				try {
					outputStream = soc.getOutputStream();
					branchMesssageBuilder.build().writeDelimitedTo(outputStream);
				} catch (IOException e) {
					e.printStackTrace();
				}
	    }
	}
	
	/**
	 * Implementation details after receiving first marker message
	 * Step 1: If it is first marker message(For particular snapshot id)
	 * 			a: Record its own state(balance)
	 * 			b: Mark channel as empty 
	 */
	private void receiveMarkerMessage(int snapshotId, String fromBranch) {
		
		//If Key is present, first marker message already received
		if(branchState.get(snapshotId) != null){
			//System.out.println();
			//System.out.println("------------Received marker from-------- " + fromBranch );
			recordFinalChannelStateAndStopRecording(snapshotId,fromBranch);
			
		}else {
			//First Marker message
			//System.out.println();
			//System.out.println("-------------My First Marker message--------from " +snapshotId + ":" + fromBranch);
			//Record local state
			recordLocalState(snapshotId);
			
			//Make incoming channel empty
			createMapForIncomingChannels(snapshotId);
			incomingChannelStates.get(snapshotId).put(fromBranch, 0);
			//printChannelMap(incomingChannelStates.get(snapshotId));
			recordFinalChannelStateAndStopRecording(snapshotId,fromBranch);
			
			//send marker messages
			Bank.Marker.Builder markerMsg = Bank.Marker.newBuilder();
			markerMsg.setSnapshotId(snapshotId);
			sendMarkerMessagesToOtherBranches(markerMsg);
			
			//start recording for other channels
		}
	}
	
	private void recordFinalChannelStateAndStopRecording(int snapshotId, String fromBranch) {
		//System.out.println("-----==copying to final==-----");
		Map<String, Integer> incomingChannelMap; 
		if(finalChannelStates.get(snapshotId) == null) {
			incomingChannelMap = new HashMap<>();
			finalChannelStates.put(snapshotId, incomingChannelMap);
		}
		//System.out.println("Size of final map before " + finalChannelStates.get(snapshotId).size());
		
		if(incomingChannelStates.get(snapshotId) != null) {
			int recordedBalance = incomingChannelStates.get(snapshotId).get(fromBranch);
			
			//copy final state after recording is stopped.
			finalChannelStates.get(snapshotId).put(fromBranch, recordedBalance);
			printChannelMap(finalChannelStates.get(snapshotId), snapshotId);
			//System.out.println("Size of final map after " + finalChannelStates.get(snapshotId).size());
			
			
			//stop recording i.e remove entry from incomingChannelStates
			incomingChannelStates.get(snapshotId).remove(fromBranch);
		}
	}
	
	/**
	 * For all recording branches update channel amount
	 * @param amount
	 * @param fromBranch
	 */
	private void updateAmountForAllRecordingChannels(int amount, String fromBranch) {
		Iterator it = incomingChannelStates.entrySet().iterator();
	    while (it.hasNext()) {
	        Map.Entry pair = (Map.Entry)it.next();
	        Map<String, Integer> channelMap = new HashMap<>();
	        channelMap = incomingChannelStates.get(pair.getKey());
	        
	        if(channelMap.get(fromBranch) != null) {
	        	int updatedBal;
	        	
	        	if(channelMap.get(fromBranch) == -1)
	        		updatedBal = amount;
	        	else
		        	updatedBal = amount + channelMap.get(fromBranch);

	        	channelMap.put(fromBranch, updatedBal);
	        	incomingChannelStates.put((Integer) pair.getKey(), channelMap);
	        }
	    }
		
	}
	
	/**
	 * Create a Map for recording incoming channels for given snapshotId
	 * @param snapshotId
	 */
	private void createMapForIncomingChannels(int snapshotId) {
		if(incomingChannelStates.get(snapshotId) == null) {
			Map<String, Integer> incomingChannelMap = getInitializeMap();
			incomingChannelStates.put(snapshotId, incomingChannelMap);
		}
		//printChannelMap(incomingChannelStates.get(snapshotId));
	}
	
	/**
	 * record balance of branch as local state when snapshot has initiated
	 * @param snapshotId
	 */
	private void recordLocalState(int snapshotId) {
		branchState.put(snapshotId, branchBalance);
	}
	
	private Map<String, Integer> getInitializeMap(){
		Map<String, Integer> channelMap = new HashMap<>();

		Iterator it = map.entrySet().iterator();
	    while (it.hasNext()) {
	        Map.Entry pair = (Map.Entry)it.next();
	        channelMap.put((String) pair.getKey(), -1);
	    }
	    return channelMap;
	}
	
	private Bank.ReturnSnapshot returnSnapshotToController(int snapshotId) {
		System.out.println("------------Retrieving snapshot for-:: " + snapshotId);
		//System.out.println("Size of final channel state map ::" + finalChannelStates.get(snapshotId).size());
		Bank.ReturnSnapshot.Builder returnBuilder = Bank.ReturnSnapshot.newBuilder();
		
		Bank.ReturnSnapshot.LocalSnapshot.Builder localBuilder = Bank.ReturnSnapshot.LocalSnapshot.newBuilder();
		localBuilder.setSnapshotId(snapshotId);
		localBuilder.setBalance(branchState.get(snapshotId));
		
		System.out.println(branchName + ": " + branchState.get(snapshotId));
		Map<String, Integer> channelMap = finalChannelStates.get(snapshotId);
		
		List<Integer> list = new ArrayList<>();
		for(Bank.InitBranch.Branch branch : allBranchDetails.getInitBranch().getAllBranchesList()) {
			if(channelMap.get(branch.getName()) != null) {
				System.out.println(branchName + "<-" + branch.getName() + ":" + channelMap.get(branch.getName()));
				
				if(channelMap.get(branch.getName()) == -1)
					list.add(0);
				else
					list.add(channelMap.get(branch.getName()));
			}
		}
		
		localBuilder.addAllChannelState(list);
		
		returnBuilder.setLocalSnapshot(localBuilder);
		
		return returnBuilder.build();
	}
	
	private void printChannelMap(Map<String, Integer> channelMap, int snapId) {
		System.out.println();
		System.out.println("----------------Final Map::-" + snapId);
		Iterator it = channelMap.entrySet().iterator();
		
	    while (it.hasNext()) {
	        Map.Entry pair = (Map.Entry)it.next();
	        System.out.println(pair.getKey() + "::::" + pair.getValue());
	    }
	}
	
	/**
	 * This method is used to initialize initial branch balance, branch balance and other branch details
	 * @param branchMsg
	 */
	private void initializeBranchDetails(Bank.BranchMessage branchMsg) {
		
		branchInitialBalance = branchMsg.getInitBranch().getBalance();
		branchBalance = branchInitialBalance;
		
		allBranchDetails = branchMsg;
		
		//initialize incoming channel states
		System.out.println("---------------------------------");
		
		System.out.println("Branch initial amount " + branchBalance);
		
		for(Bank.InitBranch.Branch branch : allBranchDetails.getInitBranch().getAllBranchesList()) {
			//System.out.println(branch.getName() + " " + branch.getIp() + " " + branch.getPort());
		}
		
		System.out.println("---------------------------------");
		System.out.println();
	}
	
	
	/**
	 * This method generates a random number and returns the branch name of the index of generated random number
	 * @return
	 */
	private String getRandomBranchName() {
		String name = null;
		
		int totalBranches = allBranchDetails.getInitBranch().getAllBranchesCount();

		//nextInt(min,max) -> min is inclusive and max is exclusive
		int index = ThreadLocalRandom.current().nextInt(0, totalBranches);
		
		while(allBranchDetails.getInitBranch().getAllBranches(index).getName().equals(branchName)) {
			index = ThreadLocalRandom.current().nextInt(0, totalBranches);
		}

		name = allBranchDetails.getInitBranch().getAllBranches(index).getName();
		return name;
	}
	
	/**
	 * Get random amount between 1% to 5% of branch initial amount. Return 0 if amount cannot be transfered
	 * @return
	 */
	private int getAmountToTransfer() {
		int amount = 0;
		int low = (int) (0.01*branchInitialBalance);
		int high = (int) (0.05*branchInitialBalance);
		
		//nextInt(min,max) -> min is inclusive and max is exclusive
		amount = ThreadLocalRandom.current().nextInt(low, high+1);
		System.out.println();
		synchronized (lock) {
			if((branchBalance-amount) > 0) {
				System.out.println("Before Transfer:: " + branchBalance);
					branchBalance = branchBalance - amount;
			}else
				amount = 0;
		}
		
		return amount;
	}
	
	/**
	 * Update branch balance by specified amount
	 * @param amount
	 */
	private void updateBalance(int amount){
		System.out.println();
		
		synchronized (lock) {
			System.out.print("Received(" + branchBalance + "+" + amount + ")");
			branchBalance = branchBalance + amount;
		}
		System.out.print(" = " + branchBalance);
		System.out.println();
	}
	
	private void transferMoneyToAllBranches() {
				
		Thread sendMoneyThread = new Thread() {
			public void run() {
				System.out.println("Thread to transfer money started");
				while(true){
					//Transfer amount 
					long sleepTime = (long)(Math.random()*(5000));
					try {
						Thread.sleep(sleepTime);
					} catch (InterruptedException e1) {
						e1.printStackTrace();
					}
					
					String branch = getRandomBranchName();
					Socket clientSocket;
					try {
						clientSocket = map.get(branch);
						Bank.Transfer.Builder transferMsgBuilder = Bank.Transfer.newBuilder();
						int transferAmount = getAmountToTransfer();
						
						if(transferAmount > 0) {
							transferMsgBuilder.setMoney(transferAmount);
							
							Bank.BranchMessage.Builder branchMsgBuilder  = Bank.BranchMessage.newBuilder();
							branchMsgBuilder.setTransfer(transferMsgBuilder);
							
							System.out.println("Transfering:(" + transferAmount + "->" + branch + ")==" + branchBalance);
							branchMsgBuilder.build().writeDelimitedTo(clientSocket.getOutputStream());
						}
						
					} catch (UnknownHostException e) {
						e.printStackTrace();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}
		};
		
		sendMoneyThread.start();
	}
		
}
