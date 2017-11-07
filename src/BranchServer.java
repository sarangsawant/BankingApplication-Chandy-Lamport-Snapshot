import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;

public class BranchServer {

	static int branchBalance=0;
	int branchInitialBalance=0;	
	static String branchIPAddress;
	static String branchName;
	static int branchPort;
	Bank.BranchMessage allBranchDetails;
	static Map<String, Socket> map = new HashMap<>();
	static int establishedTcpConnections = 0;
	
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
					
						BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
						String name = in.readLine();
						
						String ip = socket.getInetAddress().getHostAddress();
						int port = socket.getPort();
						
						//System.out.println("Received connection from " + name + " " + ip + " " + port);
						map.put(name, socket);
						establishedTcpConnections+=1;
						
						branchServer.syncTCPConnectionsAndStartMoneyTransfer();
						
						new BranchHandler(socket).start();
						
				} catch (IOException e) {
					e.printStackTrace();
				}
				
			}
			
	}
	
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
				
				syncTCPConnectionsAndStartMoneyTransfer();
				
				//Establish TCP connection and wait for incoming messages
				new BranchHandler(socket).start();
				
		}
		
	}
	
	private void printMap() {
		System.out.println("----------------Map------------");
		Iterator it = map.entrySet().iterator();
	    while (it.hasNext()) {
	        Map.Entry pair = (Map.Entry)it.next();
	        System.out.println(pair.getKey() + " = " + pair.getValue());
	    }
	}
	
	private static class BranchHandler extends Thread {
        private Socket clientSocket;
        
        public BranchHandler(Socket socket) {
            clientSocket = socket;
        }
 
        public void run() {
        	try {
				
				while(true) {
					Bank.BranchMessage branchMessage = Bank.BranchMessage.parseDelimitedFrom(clientSocket.getInputStream());
					//System.out.println("Amount received-> " + branchMessage.getTransfer().getMoney());
					if(branchMessage.hasTransfer()) {
						int amount = branchMessage.getTransfer().getMoney();
						BranchServer.updateBalance(amount);
					}
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
			
        }
    }
    
	public void syncTCPConnectionsAndStartMoneyTransfer() {
		int expectedConnections = allBranchDetails.getInitBranch().getAllBranchesCount()-1;
		
		if(expectedConnections == establishedTcpConnections) {
			System.out.println("Yeah!!!All connections established, Starting transactions");
			printMap();
			transferMoneyToAllBranches();
		}
		
	}
	private void initializeBranchDetails(Bank.BranchMessage branchMsg) {
		
		branchInitialBalance = branchMsg.getInitBranch().getBalance();
		branchBalance = branchInitialBalance;
		
		allBranchDetails = branchMsg;
		
		System.out.println("Branch initial amount " + branchBalance);
		
		for(Bank.InitBranch.Branch branch : allBranchDetails.getInitBranch().getAllBranchesList()) {
			System.out.println(branch.getName() + " " + branch.getIp() + " " + branch.getPort());
		}
	}
	
	
	/**
	 * 
	 * @return
	 */
	private String getRandomBranchName() {
		String name = null;
		
		int totalBranches = allBranchDetails.getInitBranch().getAllBranchesCount();
		Random random = new Random();
		
		//(High-low)+low, High is size-1, low is 0
		int index = random.nextInt((totalBranches-1) - 0);
		
		while(allBranchDetails.getInitBranch().getAllBranches(index).getName().equals(branchName)) {
			index = random.nextInt((totalBranches-1) - 0);
		}

		name = allBranchDetails.getInitBranch().getAllBranches(index).getName();
		return name;
	}
	
	/**
	 * Get random amount between 1% to 5% of branch initial amount. Return 0 if amount cannot be transfered
	 * @return
	 */
	private synchronized int getAmountToTransfer() {
		int amount = 0;
		int low = (int) (0.01*branchInitialBalance);
		int high = (int) (0.05*branchInitialBalance);
		Random random = new Random();
		amount = random.nextInt(high-low)+low;
		if((branchBalance-amount) > 0) {
			branchBalance = branchBalance - amount;
		}else
			amount = 0;
		
		return amount;
	}
	
	/**
	 * Update branch balance by specified amount
	 * @param amount
	 */
	private static synchronized void updateBalance(int amount){
		System.out.println();
		System.out.println("Money Received!! Updating balance " + branchBalance + "+" + amount);
		branchBalance = branchBalance + amount;
		System.out.println("Balance " + branchName + "::"+branchBalance);
		System.out.println();
	}
	
	private void transferMoneyToAllBranches() {
				
		while(true){
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
					
					System.out.println("Transfering money " + branchName + "->" +branch + "::" + transferAmount);
					System.out.println("Current Balance:: " + branchBalance);
					System.out.println();
					
					branchMsgBuilder.build().writeDelimitedTo(clientSocket.getOutputStream());
				}
				
				long sleepTime = (long)(Math.random()*(5000));
				Thread.sleep(sleepTime);
			} catch (UnknownHostException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
	}
		

}
