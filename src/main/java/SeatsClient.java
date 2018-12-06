import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

public class SeatsClient {
	private Connection connect = null;
	private Statement stmt = null;
	private PreparedStatement preparedStatement = null;
	private ResultSet rs = null;
	private int _ISOLATION = Connection.TRANSACTION_READ_UNCOMMITTED;
	private int insID;
	Properties p;

	public SeatsClient(int insID) {
		this.insID = insID;
		p = new Properties();
		p.setProperty("ID", String.valueOf(insID));
		p.setProperty("dbName", "feedback");
	}

	/*
	 * 
	 * 
	 * 
	 * 
	 * 
	 * 
	 * 
	 * 
	 * 
	 * 
	 * 
	 */

	public long deleteReservation(int key) throws Exception {
		long beginTime = System.currentTimeMillis();
		try {
			// input arguments (set manually)
			long f_id = 13;
			long given_c_id = 99;
			String c_id_str = "!0!";
			String ff_c_id_str;
			long ff_al_id;
			int cidGiven = 0;
			Class.forName("com.github.adejanovski.cassandra.jdbc.CassandraDriver");
			System.out.println("connecting...");
			connect = DriverManager.getConnection("jdbc:cassandra://localhost" + ":1904" + insID + "/testks");
			System.out.println("connected: " + connect);
			beginTime = System.currentTimeMillis();
			if (cidGiven == 0) {
				boolean has_al_id = false;
				// Use the customer's id as a string
				if (c_id_str != "" && c_id_str.length() > 0) {

					PreparedStatement stmt1 = connect
							.prepareStatement("SELECT C_ID FROM CUSTOMER WHERE C_ID_STR = ?  ALLOW FILTERING");
					stmt1.setString(1, c_id_str);
					ResultSet results1 = stmt1.executeQuery();
					if (results1.next())
						given_c_id = results1.getLong("C_ID");
				}
			}

			PreparedStatement stmt2 = connect.prepareStatement(
					"SELECT C_SATTR00, C_SATTR02, C_SATTR04, C_IATTR00, C_IATTR02, C_IATTR04, C_IATTR06, C_BALANCE, C_IATTR10, C_IATTR11 FROM CUSTOMER WHERE C_ID = ?");
			stmt2.setLong(1, given_c_id);
			ResultSet results2 = stmt2.executeQuery();
			if (results2.next()) {
				int oldBal = results2.getInt("C_BALANCE");
				int oldAttr10 = results2.getInt("C_IATTR10");
				int oldAttr11 = results2.getInt("C_IATTR11");
				long c_iattr00 = results2.getLong("C_IATTR00") + 1;
			} else {
				results2.close();
				throw new Exception("invalid customer");
			}

			// select flight
			PreparedStatement stmt31 = connect.prepareStatement("SELECT F_SEATS_LEFT FROM FLIGHT WHERE F_ID = ? ");
			stmt31.setLong(1, f_id);
			ResultSet results3 = stmt31.executeQuery();
			if (results3.next()) {
				int seats_left = results3.getInt("F_SEATS_LEFT");

			} else {
				results2.close();
				// throw new Exception("invalid flight");
			}
			given_c_id = 30;
			// select reservation
			PreparedStatement stmt32 = connect.prepareStatement(
					"SELECT R_ID, R_SEAT, R_PRICE, R_IATTR00 FROM RESERVATION WHERE R_C_ID = ? ALLOW FILTERING");
			stmt32.setLong(1, given_c_id);
			ResultSet results4 = stmt32.executeQuery();
			long r_id = -1;
			int updated = -1;
			if (results4.next()) {
				r_id = results4.getLong("R_ID");
				double r_price = results4.getInt("R_PRICE");
				results4.close();
				updated = 0;
			} else {
				System.out.println("invalid reservation");
			}

			/*
			 * 
			 * 
			 * 
			 * 
			 * 
			 * 
			 * 
			 * 
			 * 
			 * 
			 * 
			 * 
			 * 
			 * 
			 * 
			 * 
			 */
			//Thread.sleep(2500);
			// Now delete all of the flights that they have on this flight
			PreparedStatement stmt4 = connect
					.prepareStatement("DELETE FROM RESERVATION WHERE R_ID = ? AND R_C_ID = ? AND R_F_ID = ?");
			stmt4.setLong(1, r_id);
			stmt4.setLong(2, given_c_id);
			stmt4.setLong(3, f_id);
			updated = stmt4.executeUpdate();
			System.out.println(">>> DELETE#1 end");
			/*
			 * 
			 * 
			 * 
			 * 
			 * 
			 * 
			 * 
			 * 
			 * 
			 * 
			 * 
			 * 
			 * 
			 * 
			 * 
			 * 
			 * 
			 * 
			 * 
			 * 
			 * 
			 */

		} catch (Exception e) {
			throw e;
		} finally {
			close();
		}
		return System.currentTimeMillis() - beginTime;
	}

	/*
	 * 
	 * 
	 * 
	 * 
	 * 
	 * 
	 * 
	 * 
	 * 
	 * 
	 * 
	 */

	public long newReservation(int key) throws Exception {
		long beginTime = System.currentTimeMillis();
		try {
			// input arguments (set manually)
			int r_id = 0;
			int c_id = 30;
			int f_id = 13;
			int seatnum = 20;
			int price = 0;
			int attrs[] = { 69, 69, 69, 69, 69, 69, 69, 69, 69, 69, 69, 69, 69 };
			Class.forName("com.github.adejanovski.cassandra.jdbc.CassandraDriver");
			System.out.println("connecting...");
			connect = DriverManager.getConnection("jdbc:cassandra://localhost" + ":1904" + insID + "/testks");
			System.out.println("connected: " + connect);
			beginTime = System.currentTimeMillis();
			// Flight Information
			PreparedStatement stmt11 = connect
					.prepareStatement("SELECT F_AL_ID, F_SEATS_LEFT FROM FLIGHT WHERE F_ID = ?");
			stmt11.setInt(1, f_id);
			ResultSet rs1 = stmt11.executeQuery();
			boolean found1 = rs1.next();

			// Airline Information
			PreparedStatement stmt12 = connect.prepareStatement("SELECT * FROM AIRLINE WHERE AL_ID = ?");
			stmt12.setInt(1, f_id);
			ResultSet rs2 = stmt12.executeQuery();
			boolean found2 = rs2.next();
			int seats_left = -1;
			int airline_id = -1;
			if (!found1 || !found2) {
				System.out.println("Invalid flight");
			} else {
				airline_id = rs1.getInt("F_AL_ID");
				seats_left = rs1.getInt("F_SEATS_LEFT");
				rs.close();
				if (seats_left <= 0) {
					System.out.println(" No more seats available for flight");
				}
			}

			/*
			 * 
			 * 
			 * 
			 * 
			 * 
			 * 
			 * 
			 * 
			 * 
			 * 
			 * 
			 * 
			 * 
			 * 
			 * 
			 * 
			 * 
			 * 
			 * 
			 * 
			 * 
			 */
			// Check if Seat is Available
			PreparedStatement stmt2 = connect
					.prepareStatement("SELECT R_ID FROM RESERVATION WHERE R_F_ID = ? and R_SEAT = ? ALLOW FILTERING");
			stmt2.setInt(1, f_id);
			stmt2.setInt(2, seatnum);
			ResultSet rs3 = stmt2.executeQuery();
			boolean found3 = rs3.next();
			if (found3)
				System.out.println("1");
			else
				System.out.println("0");

			/*
			 * 
			 */
			//Thread.sleep(5000);
			/*
			 * 
			 */
			// Check if the Customer already has a seat on this flight
			PreparedStatement stmt3 = connect.prepareStatement(
					"SELECT R_ID " + "  FROM RESERVATION WHERE R_F_ID = ? AND R_C_ID = ? ALLOW FILTERING");
			stmt3.setInt(1, f_id);
			stmt3.setInt(2, c_id);
			ResultSet rs4 = stmt3.executeQuery();
			boolean found4 = rs4.next();
			if (!found4)
				System.out.println("0");
			else
				System.out.println("1");
			/*
			 * 
			 * 
			 * 
			 * 
			 * 
			 * 
			 * 
			 * 
			 * 
			 * 
			 * 
			 * 
			 * 
			 * 
			 * 
			 * 
			 * 
			 * 
			 * 
			 * 
			 * 
			 * 
			 * 
			 * 
			 * 
			 */

			if (found4)
				throw new Exception(
						String.format(" Customer %d already owns a reservations on flight #%d", c_id, f_id));

			else
				System.out.println("customer is eligble to book!");

			// Get Customer Information
			PreparedStatement stmt4 = connect.prepareStatement(
					"SELECT C_BASE_AP_ID, C_BALANCE, C_SATTR00, C_IATTR10, C_IATTR11 FROM CUSTOMER WHERE C_ID = ? ");
			stmt4.setInt(1, c_id);
			ResultSet rs5 = stmt4.executeQuery();
			int oldAttr10 = -1;
			int oldAttr11 = -1;
			if (rs5.next()) {
				oldAttr10 = rs5.getInt("C_IATTR10");
				oldAttr11 = rs5.getInt("C_IATTR11");
			} else {
				// throw new Exception(String.format(" Invalid customer id: %d / %s", c_id,
				// c_id));
			}

			PreparedStatement stmt5 = connect.prepareStatement(
					"INSERT INTO RESERVATION (R_ID, R_C_ID, R_F_ID, R_SEAT, R_PRICE, R_IATTR00, R_IATTR01, "
							+ "   R_IATTR02, R_IATTR03, R_IATTR04, R_IATTR05, R_IATTR06, R_IATTR07, R_IATTR08) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
			stmt5.setInt(1, r_id);
			stmt5.setInt(2, c_id);
			stmt5.setInt(3, f_id);
			stmt5.setInt(4, seatnum);
			stmt5.setInt(5, 2);
			stmt5.setInt(6, attrs[0]);
			stmt5.setInt(7, attrs[1]);
			stmt5.setInt(8, attrs[2]);
			stmt5.setInt(9, attrs[3]);
			stmt5.executeUpdate();

			PreparedStatement stmt6 = connect
					.prepareStatement("UPDATE FLIGHT SET F_SEATS_LEFT = ? " + " WHERE F_ID = ? ");
			stmt6.setInt(1, seats_left - 1);
			stmt6.setInt(2, f_id);
			stmt6.executeUpdate(); // update
			// customer
			PreparedStatement stmt7 = connect.prepareStatement(
					"UPDATE CUSTOMER SET C_IATTR10 = ?, C_IATTR11 = ?, C_IATTR12 = ?, C_IATTR13 = ?, C_IATTR14 = ?, C_IATTR15 = ?"
							+ "  WHERE C_ID = ? ");

			stmt7.setInt(1, oldAttr10 + 1);
			stmt7.setInt(2, oldAttr11 + 1);
			stmt7.setInt(3, attrs[0]);
			stmt7.setInt(4, attrs[1]);
			stmt7.setInt(5, attrs[2]);
			stmt7.setInt(6, attrs[3]);
			stmt7.setInt(7, c_id);
			stmt7.executeUpdate(); // update
									// frequent
									// flyer
			PreparedStatement stmt81 = connect
					.prepareStatement("SELECT FF_IATTR10 FROM FREQUENT_FLYER WHERE FF_C_ID = ? AND FF_AL_ID = ?");
			stmt81.setInt(1, c_id);
			stmt81.setInt(2, airline_id);
			ResultSet rs6 = stmt81.executeQuery();
			int oldFFAttr10 = -10;
			if (rs6.next())
				oldFFAttr10 = rs6.getInt("FF_IATTR10");

			PreparedStatement stmt82 = connect.prepareStatement(
					"UPDATE FREQUENT_FLYER SET FF_IATTR10 = ?, FF_IATTR11 = ?, FF_IATTR12 = ?, FF_IATTR13 = ?, FF_IATTR14 = ? "
							+ " WHERE FF_C_ID = ? " + "   AND FF_AL_ID = ?");
			System.out.println(">>>" + attrs.length);
			stmt82.setInt(1, oldFFAttr10 + 1);
			stmt82.setInt(2, attrs[4]);
			stmt82.setInt(3, attrs[5]);
			stmt82.setInt(4, attrs[6]);
			stmt82.setInt(5, attrs[7]);
			stmt82.setInt(6, c_id);
			stmt82.setInt(7, airline_id);
			stmt82.executeUpdate();

		} catch (

		Exception e) {
			throw e;
		} finally {
			close();
		}
		return System.currentTimeMillis() - beginTime;

	}

	/*
	 * 
	 * 
	 * 
	 * 
	 * 
	 * 
	 * 
	 * 
	 * 
	 * 
	 * 
	 */

	public long initialize(int key) throws Exception {
		try {

			Class.forName("com.github.adejanovski.cassandra.jdbc.CassandraDriver");
			System.out.println("connecting...");
			connect = DriverManager.getConnection("jdbc:cassandra://localhost" + ":1904" + insID + "/testks");
			System.out.println("connected: " + connect);
			preparedStatement = connect.prepareStatement(
					"insert into reservation (r_id,r_c_id,r_f_id,r_seat,r_price,r_iattr00,r_iattr01,r_iattr02,r_iattr03,r_iattr04,r_iattr05,r_iattr06,r_iattr07,r_iattr08) values (29,30,13,20,0,84,84,84,84,84,84,84,84,84)");
			preparedStatement.executeUpdate();

			preparedStatement = connect.prepareStatement(
					"insert into customer (C_ID, C_ID_STR,C_BASE_AP_ID, C_BALANCE ,C_SATTR00, C_SATTR01,C_SATTR02,C_SATTR03,C_SATTR04,C_SATTR05,C_SATTR06,"
							+ "C_SATTR07,C_SATTR08 ,C_SATTR09,C_SATTR10,C_SATTR11,C_SATTR12,C_SATTR13,C_SATTR14,C_SATTR15 ,C_SATTR16,C_SATTR17,C_SATTR18,C_SATTR19,"
							+ "C_IATTR00 ,C_IATTR01,C_IATTR02,C_IATTR03,C_IATTR04,C_IATTR05,C_IATTR06,C_IATTR07,C_IATTR08,C_IATTR09,C_IATTR10,C_IATTR11,C_IATTR12,"
							+ "C_IATTR13,C_IATTR14,C_IATTR15,C_IATTR16,C_IATTR17, C_IATTR18,C_IATTR19) "
							+ "values (0,'!0!',0,0,'!s!','!s!','!s!','!s!','!s!','!s!','!s!','!s!','!s!','!s!','!s!','!s!','!s!','!s!','!s!','!s!','!s!','!s!','!s!','!s!',0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0)");
			preparedStatement.executeUpdate();

		} catch (Exception e) {
			throw e;
		} finally {
			close();
		}
		return -1;
	}

	private void close() {
		try {
			if (rs != null)
				rs.close();
			if (rs != null)
				rs.close();
			if (connect != null)
				connect.close();
		} catch (Exception e) {

		}
	}

}
