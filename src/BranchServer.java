import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

public class BranchServer {

	int branchBalance=0;	
	static String branchIPAddress;
	static String branchName;
	static int branchPort;
	
	@SuppressWarnings("unchecked")
	public static void main(String[] args) {
		if(args.length != 2){
			System.out.println("please provide a valid branch-name and port");
			System.exit(0);
		}
		try {
			branchIPAddress = InetAddress.getLocalHost().getHostAddress();
			branchName = args[0];
			branchPort = Integer.valueOf(args[1]);
			
			ServerSocket serverSocket = null;
			try {
				serverSocket = new ServerSocket(branchPort);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			System.out.println("Branch server started " + branchIPAddress + " " + branchPort);
			BranchServer branchServer = new BranchServer();
			Bank.BranchMessage branchMsg = null;
			InputStream inputStream;
			DataInputStream in; 
			ObjectInputStream objectInputStream = null;
			List<String> list = new ArrayList<>();
			while(true) {
				try {
					System.out.println("Listining on port " + branchPort);
					Socket socket = serverSocket.accept();
					
					try {
						inputStream = socket.getInputStream();
						
						if(branchMsg == null) {
							branchMsg = Bank.BranchMessage.parseFrom(inputStream);
							
							if(branchMsg.hasInitBranch())
								branchServer.initializeBranchDetails(branchMsg);
						
						//if(branchMsg.hasTransfer()) {
							//new Thread
						//}
						}
						else {
							objectInputStream = new ObjectInputStream(inputStream);
							list = (List<String>) objectInputStream.readObject();
							System.out.println("list " + list.toString());
						}
						
						 
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (ClassNotFoundException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
						
					//new Thread(new BranchServerHandler(socket, branchIPAddress, branchName , branchPort)).start();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
			}
			
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
	}
	
	private void initializeBranchDetails(Bank.BranchMessage branchMsg) {
		
		branchBalance = branchMsg.getInitBranch().getBalance();
		
		System.out.println("Branch initial amount " + branchBalance);
		
		for(Bank.InitBranch.Branch branch : branchMsg.getInitBranch().getAllBranchesList()) {
			System.out.println(branch.getName() + " " + branch.getIp() + " " + branch.getPort());
		}
	}
	
	/*private void transferMoneyToAllBranches() {
		
		for(Bank.InitBranch.Branch branch : branchMsg.getInitBranch().getAllBranchesList()) {
			
			//Exclude itself before making tcp connection to all branches
			if(! (branch.getName().equals(branchName) && branch.getIp().equals(branchIPAddress) && (branch.getPort()==branchPort)) ) {
				Socket clientSocket;
				try {
					clientSocket = new Socket(branch.getIp(), branch.getPort());
					Bank.Transfer.Builder transferMsgBuilder = Bank.Transfer.newBuilder();
					transferMsgBuilder.setMoney(10);
					
					Bank.BranchMessage.Builder branchMsgBuilder  = Bank.BranchMessage.newBuilder();
					branchMsgBuilder.setTransfer(transferMsgBuilder);
					
					OutputStream outputStream = clientSocket.getOutputStream();
					branchMsgBuilder.build().writeTo(outputStream);
					
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
	*/
	private synchronized void updateBalance(int amount){
		branchBalance = branchBalance + amount;
	}
	
	
		

}
