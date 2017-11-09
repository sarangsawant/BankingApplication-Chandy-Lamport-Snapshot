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
import java.util.Random;
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
	
	private ConcurrentHashMap<Integer, Bank.ReturnSnapshot.LocalSnapshot.Builder> snapshots = new ConcurrentHashMap<Integer, Bank.ReturnSnapshot.LocalSnapshot.Builder>();
	
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
				branchMsg = Bank.BranchMessage.parseFrom(inputStream);
				if(branchMsg.hasInitBranch()) {
					branchServer.initializeBranchDetails(branchMsg);
					branchServer.setUpTCPConnections();
				}
				
			}catch (IOException e) {
				e.printStackTrace();
			}
		
			while(true) {
				try {
						Socket socket = serverSocket.accept();
						
							if(flag == 0) {
								BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
								String name = in.readLine();
								
								map.put(name, socket);
								establishedTcpConnections+=1;
								
								//Start listening for messages and then start money transfer
								new BranchHandler(socket,branchServer).start();
								branchServer.syncTCPConnectionsAndStartMoneyTransfer();	
							}else {
								Bank.BranchMessage msg = Bank.BranchMessage.parseFrom(socket.getInputStream());
								System.out.println("\nReceived snapshot message " + msg.getInitSnapshot().getSnapshotId());
								branchServer.initiateLocalSnapshotProcedure(msg.getInitSnapshot().getSnapshotId());
							}
						
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
					//System.out.println("Establishing connection  " + branchName + "->" + name);
					
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
				new BranchHandler(socket, this).start();
				syncTCPConnectionsAndStartMoneyTransfer();
		}
		
	}
	
	private static class BranchHandler extends Thread {
        private Socket clientSocket;
        private BranchServer branchServer;
        
        public BranchHandler(Socket socket, BranchServer server) {
            clientSocket = socket;
            branchServer = server;
        }
 
        public void run() {
        	try {
				/*while(true) {
					Bank.BranchMessage branchMessage = Bank.BranchMessage.parseDelimitedFrom(clientSocket.getInputStream());
					//System.out.println("Amount received-> " + branchMessage.getTransfer().getMoney());
					if(branchMessage.hasTransfer()) {
						int amount = branchMessage.getTransfer().getMoney();
						branchServer.updateBalance(amount);
					}
				}*/
        		InputStream inputStream = clientSocket.getInputStream();
        		
        		Bank.BranchMessage branchMessage = null;
        		while( (branchMessage = Bank.BranchMessage.parseDelimitedFrom(inputStream)) != null){
        			if(branchMessage.hasTransfer()) {
						int amount = branchMessage.getTransfer().getMoney();
						branchServer.updateBalance(amount);
					}
        			
        			if(branchMessage.hasMarker()) {
        				branchServer.receiveMarkerMessage();
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
			printMap();
			
			transferMoneyToAllBranches();
		}
		
	}
	
	/**
	 * This function is used to initiaite snapshot for local branch
	 * Step 1: Record its own local state (Mark as 1st marker received)
	 * step 2: Send marker Messages to other branches
	 * step 3: start recording on incoming channel
	 */
	private void initiateLocalSnapshotProcedure(int id) {
		int localState = branchBalance;
		int snapshotId = id;
		List<Integer> channelState = new ArrayList<>();
		
		Bank.ReturnSnapshot.LocalSnapshot.Builder localSnapshotBuilder = Bank.ReturnSnapshot.LocalSnapshot.newBuilder();
		localSnapshotBuilder.setBalance(localState);
		localSnapshotBuilder.setSnapshotId(snapshotId);
		localSnapshotBuilder.addAllChannelState(channelState);
		
		snapshots.put(snapshotId, localSnapshotBuilder);
		
		Bank.Marker.Builder markerMsg = Bank.Marker.newBuilder();
		markerMsg.setSnapshotId(snapshotId);
		
		sendMarkerMessagesToOtherBranches(markerMsg);
		
		//Start recording on incoming channels
		
		
	}
	
	private void sendMarkerMessagesToOtherBranches(Bank.Marker.Builder marker) {
		Bank.BranchMessage.Builder branchMesssageBuilder  = Bank.BranchMessage.newBuilder();
		branchMesssageBuilder.setMarker(marker);
		
		Iterator it = map.entrySet().iterator();
	    while (it.hasNext()) {
	        Map.Entry pair = (Map.Entry)it.next();
	        if(!pair.getKey().equals(branchName)) {
	        	System.out.println("----Sending marker message to----- " + pair.getKey());
	        	Socket soc = map.get(pair.getKey());
	        	
	        	OutputStream outputStream;
				try {
					outputStream = soc.getOutputStream();
					branchMesssageBuilder.build().writeDelimitedTo(outputStream);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
	        }
	    }
	}
	
	/**
	 * Implementation details after receiving first marker message
	 * Step 1: If it is first marker message(For particular snapshot id)
	 * 			a: Record its own state(balance)
	 * 			b: Mark channel as empty 
	 */
	private void receiveMarkerMessage() {
		System.out.println("--------Marker Message received---------");
	}
	private void printMap() {
		System.out.println("----------------Map------------");
		Iterator it = map.entrySet().iterator();
	    while (it.hasNext()) {
	        Map.Entry pair = (Map.Entry)it.next();
	        System.out.println(pair.getKey() + " = " + pair.getValue());
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
		
		System.out.println("---------------------------------");
		
		System.out.println("Branch initial amount " + branchBalance);
		
		for(Bank.InitBranch.Branch branch : allBranchDetails.getInitBranch().getAllBranchesList()) {
			System.out.println(branch.getName() + " " + branch.getIp() + " " + branch.getPort());
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
