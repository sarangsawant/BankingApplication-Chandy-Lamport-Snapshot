import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

public class Controller {

	Bank.InitBranch.Builder branchBuilder = Bank.InitBranch.newBuilder();
	static Map<String, Socket> branchConnections = new HashMap<>();

	public static void main(String[] args) {
		if(args.length != 2){
			System.out.println("Invalid number of arguments");
			System.exit(0);
		}
		try {
			int totalAmount = Integer.valueOf(args[0]);
			String fileName = args[1];
			Controller controller = new Controller();
			controller.parseFileAndInitializeBranches(totalAmount, fileName);
			
			controller.sendInitSnapshotMessageToRandomBranch();
			
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
	
	private void sendInitSnapshotMessageToRandomBranch() {
		
		Thread initSnapshotThread = new Thread(){
			
			public void run() {
				//Initiate snapshot message
				Socket socket = null;
				int snapshotId = 1;
				OutputStream outputStream = null;
				Bank.BranchMessage.Builder branchMesssageBuilder = null;
				Bank.InitSnapshot.Builder initSnapshotBuilder = null;
				Bank.RetrieveSnapshot.Builder retrieveBuilder = null;
				
				while(true) {
					//send initiate snapshot after every 2 seconds
					try {
						Thread.sleep(2000L);
					} catch (InterruptedException e1) {
						e1.printStackTrace();
					}
					
					//nextInt(min,max) -> min is inclusive and max is exclusive
					int randomIndex = ThreadLocalRandom.current().nextInt(0, branchBuilder.getAllBranchesCount());
					
					//randomly selected branch for initiating snapshot
					String ip = branchBuilder.getAllBranches(randomIndex).getIp();
					int port = branchBuilder.getAllBranches(randomIndex).getPort();
					String name = branchBuilder.getAllBranches(randomIndex).getName();

					initSnapshotBuilder = Bank.InitSnapshot.newBuilder();
					initSnapshotBuilder.setSnapshotId(snapshotId);
					
					branchMesssageBuilder  = Bank.BranchMessage.newBuilder();
					branchMesssageBuilder.setInitSnapshot(initSnapshotBuilder);
					
					//System.out.println();
					//System.out.println("Sending initiate snapshot message");
					try {
						socket = Controller.branchConnections.get(name);
						outputStream = socket.getOutputStream();
						
						//send initiate snapshot message
						branchMesssageBuilder.build().writeDelimitedTo(outputStream);
						//socket.close();
						
						/*try {
							Thread.sleep(1000L);
						} catch (InterruptedException e1) {
							e1.printStackTrace();
						}*/
						
						//System.out.println();
						//retrieve builder
						branchMesssageBuilder  = Bank.BranchMessage.newBuilder();
						retrieveBuilder = Bank.RetrieveSnapshot.newBuilder();
						retrieveBuilder.setSnapshotId(snapshotId);
						
						branchMesssageBuilder.setRetrieveSnapshot(retrieveBuilder);
						
						for(Bank.InitBranch.Branch branch : branchBuilder.getAllBranchesList()) {
							//System.out.println("Sending retrieve snapshot message " + branch.getName());
							socket = Controller.branchConnections.get(branch.getName());
							outputStream = socket.getOutputStream();
							branchMesssageBuilder.build().writeDelimitedTo(outputStream);
							
							new ControllerRetrieveSnapshotHandler(socket, branch.getName()).start();
						}
						
					} catch (UnknownHostException e) {
						e.printStackTrace();
					} catch (IOException e) {
						e.printStackTrace();
					}
					
					snapshotId = snapshotId + 1;
					
				}
				
			}
		};
		
		initSnapshotThread.start();
	}
	
	private class ControllerRetrieveSnapshotHandler extends Thread {
        private Socket clientSocket;
        private String toBranch;
        
        public ControllerRetrieveSnapshotHandler(Socket socket, String name) {
            clientSocket = socket;
            toBranch = name;
        }
 
        public void run() {
        	try {
        		InputStream inputStream = clientSocket.getInputStream();
        		//System.out.println();
        		//System.out.println("-----Received return snashot from ----" + toBranch);
        		Bank.BranchMessage branchMessage = Bank.BranchMessage.parseDelimitedFrom(inputStream);
        		if(branchMessage.hasReturnSnapshot()) {
        			List<Integer> list = branchMessage.getReturnSnapshot().getLocalSnapshot().getChannelStateList();
        			List<String> branchNames = new ArrayList<>();
        			
        			//System.out.println("list size " + list.size());
        			
        			for(int i=0; i<branchBuilder.getAllBranchesCount(); i++) {
        				if(!branchBuilder.getAllBranches(i).getName().equals(toBranch)) {
        					branchNames.add(branchBuilder.getAllBranches(i).getName());
        				}
        			}
        		
        			if(list.size() == branchNames.size()) {
        				System.out.println();
        				System.out.println("snapshot_id: " + branchMessage.getReturnSnapshot().getLocalSnapshot().getSnapshotId());
        				System.out.print(toBranch + ":" + branchMessage.getReturnSnapshot().getLocalSnapshot().getBalance() +", ");
        				
        				for(int j=0; j< branchNames.size(); j++) {
        					System.out.print(branchNames.get(j) + "->" + toBranch + ": " + list.get(j) + ", ");
        				}
        				
        				System.out.println();
        			}else {
        				System.out.println("Snapshot Incomplete");
        			}
        		}
			} catch (IOException e) {
				e.printStackTrace();
			}
			
        }
    }
	
	private void parseFileAndInitializeBranches(int amount, String fileName) {
		File file = new File(fileName);
		BufferedReader bufferedReader = null;
		if(!file.exists()) {
			System.out.println("Cannot open file " + fileName + "!!");
			System.exit(0);
		}
		
		try {
			bufferedReader = new BufferedReader(new FileReader(file));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			System.out.print("File not found..!");
			System.exit(0);
		}
		
		int branchCount = 0;
		String line = "";
		try {
			while((line = bufferedReader.readLine()) != null){
				String arr[] = line.split(" ");
				String branchName = arr[0];
				String ipAddress = arr[1];
				int port = Integer.parseInt(arr[2]);
				
				//System.out.println(branchName + " " + ipAddress + " " + port);
				Bank.InitBranch.Branch.Builder branch = Bank.InitBranch.Branch.newBuilder();
				branch.setName(branchName);
				branch.setIp(ipAddress);
				branch.setPort(port);
				
				branchBuilder.addAllBranches(branch);
				branchCount++;
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		int branchInitAmt = amount/branchCount;
		//System.out.println("Initial balance for each branch is "+ branchInitAmt);
		branchBuilder.setBalance(branchInitAmt);

		Bank.BranchMessage.Builder branchMsgBuilder  = Bank.BranchMessage.newBuilder();
		branchMsgBuilder.setInitBranch(branchBuilder);
		
		Socket clientSocket = null;
		for(Bank.InitBranch.Branch branch : branchBuilder.getAllBranchesList()) {
			try {
				clientSocket = new Socket(branch.getIp(), branch.getPort());
				OutputStream outputStream = clientSocket.getOutputStream();
				branchMsgBuilder.build().writeDelimitedTo(outputStream);
				
				//clientSocket.close();
			} catch (UnknownHostException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
			
			branchConnections.put(branch.getName(), clientSocket);
		}
		
	}

}
