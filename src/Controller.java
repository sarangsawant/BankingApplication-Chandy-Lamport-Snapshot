

public class Controller {

	public static void main(String[] args) {
		if(args.length != 2){
			System.out.println("Invalid number of arguments");
			System.exit(0);
		}
		try {
			int totalAmount = Integer.valueOf(args[0]);
			String fileName = args[1];
			System.out.println("Controller started " + totalAmount + " " + fileName);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
