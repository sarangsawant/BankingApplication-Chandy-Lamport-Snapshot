import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectOutputStream;
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
			
			controller.sendRetriveSnapshotMessage();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
	
	private static class RetriveSnapshotHandler extends Thread {
        private Socket clientSocket;
       
        
        public RetriveSnapshotHandler(Socket socket) {
            clientSocket = socket;
        }
 
        public void run() {
        	
			
        }
    }
	
	private void sendRetriveSnapshotMessage() {
		try {
			Thread.sleep(4000L);
		} catch (InterruptedException e1) {
			e1.printStackTrace();
		}
		
		Bank.BranchMessage.Builder branchMsgBuilder  = Bank.BranchMessage.newBuilder();
		Bank.RetrieveSnapshot.Builder retriveBuilder = Bank.RetrieveSnapshot.newBuilder();
		retriveBuilder.setSnapshotId(1);
		
		branchMsgBuilder.setRetrieveSnapshot(retriveBuilder);
		
		Socket clientSocket;
		for(Bank.InitBranch.Branch branch : branchBuilder.getAllBranchesList()) {
			try {
				clientSocket = new Socket(branch.getIp(), branch.getPort());
				OutputStream outputStream = clientSocket.getOutputStream();
				branchMsgBuilder.build().writeDelimitedTo(outputStream);
				
				//new RetriveSnapshotHandler(clientSocket);
				clientSocket.close();
			} catch (UnknownHostException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
	}
	
	private void sendInitSnapshotMessageToRandomBranch() {
		try {
			Thread.sleep(5000L);
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		//Initiate snapshot message
		//Map<String, Socket> branchConnections = new HashMap<>();
		Socket soc = null;
		int snapshotId = 1;
		//nextInt(min,max) -> min is inclusive and max is exclusive
		int randomIndex = ThreadLocalRandom.current().nextInt(0, branchBuilder.getAllBranchesCount());
		
		String branchName = branchBuilder.getAllBranches(randomIndex).getName();
		String ip = branchBuilder.getAllBranches(randomIndex).getIp();
		int port = branchBuilder.getAllBranches(randomIndex).getPort();
		
		System.out.println("Sending Initate snapshot message to " + branchName + " " + ip + " " + port);
		
		Bank.InitSnapshot.Builder initSnapshotBuilder = Bank.InitSnapshot.newBuilder();
		initSnapshotBuilder.setSnapshotId(snapshotId);
		
		Bank.BranchMessage.Builder branchMesssageBuilder  = Bank.BranchMessage.newBuilder();
		branchMesssageBuilder.setInitSnapshot(initSnapshotBuilder);
		
		try {
			soc = new Socket(ip, port);
			OutputStream outputStream = soc.getOutputStream();
			branchMesssageBuilder.build().writeDelimitedTo(outputStream);
			
			soc.close();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
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
		//Bank.InitBranch.Builder branchBuilder = Bank.InitBranch.newBuilder();
		try {
			while((line = bufferedReader.readLine()) != null){
				String arr[] = line.split(" ");
				String branchName = arr[0];
				String ipAddress = arr[1];
				int port = Integer.parseInt(arr[2]);
				
				System.out.println(branchName + " " + ipAddress + " " + port);
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
		System.out.println("Initial balance for each branch is "+ branchInitAmt);
		branchBuilder.setBalance(branchInitAmt);

		Bank.BranchMessage.Builder branchMsgBuilder  = Bank.BranchMessage.newBuilder();
		branchMsgBuilder.setInitBranch(branchBuilder);
		
		Socket clientSocket;
		for(Bank.InitBranch.Branch branch : branchBuilder.getAllBranchesList()) {
			try {
				clientSocket = new Socket(branch.getIp(), branch.getPort());
				OutputStream outputStream = clientSocket.getOutputStream();
				branchMsgBuilder.build().writeDelimitedTo(outputStream);
				
				clientSocket.close();
			} catch (UnknownHostException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
	}

}
