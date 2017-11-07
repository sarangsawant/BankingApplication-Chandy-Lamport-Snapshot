import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

public class Controller {

	
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
		} catch (Exception e) {
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
		Bank.InitBranch.Builder branchBuilder = Bank.InitBranch.newBuilder();
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
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		int branchInitAmt = amount/branchCount;
		System.out.println("Initial balance for each branch is "+ branchInitAmt);
		branchBuilder.setBalance(branchInitAmt);

		Bank.BranchMessage.Builder branchMsgBuilder  = Bank.BranchMessage.newBuilder();
		branchMsgBuilder.setInitBranch(branchBuilder);
		
		
		for(Bank.InitBranch.Branch branch : branchBuilder.getAllBranchesList()) {
			Socket clientSocket;
			try {
				clientSocket = new Socket(branch.getIp(), branch.getPort());
				OutputStream outputStream = clientSocket.getOutputStream();
				branchMsgBuilder.build().writeTo(outputStream);
							
				clientSocket.close();
			} catch (UnknownHostException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

}
