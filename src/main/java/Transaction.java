import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class Transaction {
	private SeatsClient client;
	private int insID;

	// constructor
	public Transaction(int insID) {
		this.insID = insID;
		client = new SeatsClient(insID);
	}

	// run the transaction wrapper
	public void run(String opType) {
		try {
			Method transaction = client.getClass().getMethod(opType, int.class);
			long time = (Long) transaction.invoke(client, this.insID);
			System.out.println("Execution time (" + opType + "): " + time);
		} catch (NoSuchMethodException e2) {
			System.err.println("Unknown Operaition Type: " + opType);
			e2.printStackTrace();
		} catch (SecurityException e2) {
			e2.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			e.printStackTrace();
		}
	}
}
