import java.net.InetAddress;
import java.net.UnknownHostException;

public class BranchServer {

	public static void main(String[] args) {
		if(args.length != 2){
			System.out.println("please provide a valid branch-name and port");
			System.exit(0);
		}
		try {
			String ipAddress = InetAddress.getLocalHost().getHostAddress();
			String name = args[0];
			int port = Integer.valueOf(args[1]);
			System.out.println("Branch server started " + name + " " + port);
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
	}
	
}
