package SQL;
import java.io.IOException;
import java.sql.*;
import java.util.*;
import org.apache.commons.collections4.multimap.*;
import org.apache.commons.collections4.*;
import java.util.concurrent.ArrayBlockingQueue;


public class sql {
	
	static void DDL() throws SQLException {
		System.out.print("DDL" + "\n");
		
		Scanner proj = new Scanner(System.in);
		System.out.println("ENTER THE SCHEMA NAME (MYSQL):");
		String project = proj.nextLine();
		
		Scanner UN = new Scanner(System.in);
		System.out.println("ENTER YOUR USERNAME (MYSQL):");
		String username = UN.nextLine();
		
		Scanner pass = new Scanner(System.in);
		System.out.println("ENTER THE YOUR PASSWORD (MYSQL):");
		String password = pass.nextLine();
		
		Connection connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/" + project, username, password);//Establishing connection
		System.out.println("Connected With the database successfully");
		Statement stmt = connection.createStatement();
		System.out.println("CREATING...");
		
		String drop_if_exists = "drop table if exists products,customers,stores, suppliers, fact, dates";
		
		String products = "create table PRODUCTS (PRODUCT_ID varchar(6), PRODUCT_NAME varchar(30), PRICE DECIMAL(5,2) DEFAULT (0.0),primary key (PRODUCT_ID))";
		String stores = "create table STORES (STORE_ID varchar(4), STORE_NAME varchar(20),primary key (STORE_ID))";
		String customers = "create table CUSTOMERS (CUSTOMER_ID varchar(4), CUSTOMER_NAME varchar(30),primary key (CUSTOMER_ID))";
		String suppliers = "create table SUPPLIERS (SUPPLIER_ID varchar(5), SUPPLIER_NAME varchar(30),primary key (SUPPLIER_ID))";
		String dates = "create table DATES (DATE_ date, D numeric(2), M numeric(2), QUART numeric(1), Y numeric(4), primary key (DATE_, D, M, QUART, Y))";
		
		String fact = "create table FACT (TRANSACTION_ID INT,PRODUCT_ID varchar(6),QUANTITY SMALLINT,"
				+ "CUSTOMER_ID varchar(4), \r\n"
				+ "STORE_ID varchar(4), \r\n"
				+ "SUPPLIER_ID varchar(5),\r\n"
				+ "Date_ date, D numeric(2), M numeric(2), QUART numeric(1),Y numeric(4), \r\n"
				+ "SALE_MONEY DECIMAL(5,2) DEFAULT (0.0),\r\n"
				+ "foreign key (PRODUCT_ID) references PRODUCTS(PRODUCT_ID),\r\n"
				+ "foreign key (CUSTOMER_ID) references CUSTOMERS(CUSTOMER_ID),\r\n"
				+ "foreign key (STORE_ID) references STORES(STORE_ID),\r\n"
				+ "foreign key (SUPPLIER_ID) references SUPPLIERS(SUPPLIER_ID),\r\n"
				+ "foreign key (DATE_, D, M, QUART, Y) references DATES(DATE_, D, M, QUART, Y)\r\n"
				+ ")";
		
		stmt.executeUpdate(drop_if_exists);
		stmt.executeUpdate(products);
		stmt.executeUpdate(customers);
		stmt.executeUpdate(stores);
		stmt.executeUpdate(suppliers);
		stmt.executeUpdate(dates);
		stmt.executeUpdate(fact);
		
		System.out.print("TABLES CREATED" + "\n");
		
		
	}
	
	static void sqlConnection() throws SQLException {
			Scanner proj = new Scanner(System.in);
			System.out.println("ENTER THE SCHEMA NAME (MYSQL):");
			String project = proj.nextLine();
			
			Scanner UN = new Scanner(System.in);
			System.out.println("ENTER YOUR USERNAME (MYSQL):");
			String username = UN.nextLine();
			
			Scanner pass = new Scanner(System.in);
			System.out.println("ENTER THE YOUR PASSWORD (MYSQL):");
			String password = pass.nextLine();
			
			Connection connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/" + project, username, password);//Establishing connection
			System.out.println("Connected With the database successfully");
			System.out.println("JOINING...");
			
			Statement stmt = connection.createStatement();
			Statement stmt2 = connection.createStatement();
			
			Statement prod_id = connection.createStatement();
			Statement cust_id = connection.createStatement();
			Statement st_id = connection.createStatement();
			Statement su_id = connection.createStatement();
			Statement dates_id = connection.createStatement();
			

			
			List<Map <String, String>> dataset =new ArrayList <Map <String, String>>();
			
			
			
			ArrayBlockingQueue<List<Map<String,String>>> q1 = new ArrayBlockingQueue<List<Map<String,String>>>(10);
			MultiValuedMap<String,Map<String,String>> map = new ArrayListValuedHashMap<>();
			
			List<String> md = new ArrayList<String>();
			int tlimit1 = 0;
			int tlimit2 = 50;
			int limit1 = 0;
			int limit2 = 10;
			int counter = 0;
			
		
			
			
			while(true)
			{
				if (counter == 10000)
				{
					break;
				}
				if (limit1 == 100)
				{
					limit1 = 0;
				}

				
				String query2 = "select * from transactions " + " limit " + tlimit1 + ", " + tlimit2;
				ResultSet rs = stmt.executeQuery(query2);
				
				List <Map<String, String>> stream = new ArrayList<Map<String, String>>(); 
				
				
				int p = 0;
				while(rs.next())
				{
				
					
					Map <String, String> data1 = new HashMap<String,String>();
					
					String pid = rs.getString("PRODUCT_ID");
					String tid = rs.getString("TRANSACTION_ID");
					String cid = rs.getString("CUSTOMER_ID");
					String cname = rs.getString("CUSTOMER_NAME");
					String storeid = rs.getString("STORE_ID");
					String storename = rs.getString("STORE_NAME");
					String tdate = rs.getString("T_DATE");
					String quan = rs.getString("QUANTITY");
					
					
					
					//q1.add(dataset);
					
					data1.put("PID", pid);
					data1.put("TRANSACTION_ID", tid);
					data1.put("CUSTOMER_ID", cid);
					data1.put("CUSTOMER_NAME", cname);
					data1.put("STORE_ID", storeid);
					data1.put("STORE_NAME", storename);
					data1.put("T_DATE", tdate);
					data1.put("QUANTITY", quan);
		
					dataset.add(data1);
					stream.add(data1);
					
					
					map.put(data1.get("PID"), data1);
					
					p++;
					
					
				}
				
				int size_required = 10;
				if(q1.size() == size_required)
				{
					
					for(Map<String,String> x : q1.remove())
					{
						String PID_check = x.get("PID");
						
						map.removeMapping(PID_check, x);
						
					}
				}
//				
				q1.add(stream);
////				
////				
//				int limit1 = 0;
//				int limit2 = 10;
				String query = "select * from masterdata limit " + limit1 + ", " + limit2;  
				ResultSet ms = stmt.executeQuery(query);
				while(ms.next())
				{
					Map<String,String> data2=new HashMap<String,String>();
					String pid = ms.getString("PRODUCT_ID");
					String pname = ms.getString("PRODUCT_NAME");
					String sid = ms.getString("SUPPLIER_ID");
					String sname = ms.getString("SUPPLIER_NAME");
					String price = ms.getString("PRICE");
					
					data2.put("PID", pid);
					data2.put("PRODUCT_NAME", pname);
					data2.put("SUPPLIER_ID", sid);
					data2.put("SUPPLIER_NAME", sname);
					data2.put("PRICE", price);

					for(Map<String,String> x:map.get(pid))
					{						
						float pr = Float.parseFloat(data2.get("PRICE"));
						float quan = Float.parseFloat(x.get("QUANTITY"));
						
						String dates = x.get("T_DATE");
						String[] d = dates.split("-");
						
						String supid = data2.get("SUPPLIER_ID");
						String supname = data2.get("SUPPLIER_NAME");
						//System.out.print(supid + ", " + supname + "\n");
						String new_supname = supname.replace("'", "");
						
						String pcheck = "SELECT * from PRODUCTS WHERE PRODUCT_ID = '" + data2.get("PID") + "'";
						String ccheck = "SELECT * from CUSTOMERS WHERE CUSTOMER_ID = '" + x.get("CUSTOMER_ID") + "'";
						String stcheck = "SELECT * from STORES WHERE STORE_ID = '" + x.get("STORE_ID") + "'";
						String supcheck = "SELECT * from SUPPLIERS WHERE SUPPLIER_ID = '" + supid + "'";
						String datescheck = "SELECT * from DATES WHERE DATE_ = '" + dates + "'";
						
						ResultSet pc = prod_id.executeQuery(pcheck);
						ResultSet cc = cust_id.executeQuery(ccheck);
						ResultSet stc = st_id.executeQuery(stcheck);
						ResultSet supc = su_id.executeQuery(supcheck);
						ResultSet datesc = dates_id.executeQuery(datescheck); 
						
						Statement stmtp = connection.createStatement();
						Statement stmtc = connection.createStatement();
						Statement stmtst = connection.createStatement();
						Statement stmtsup = connection.createStatement();
						Statement stmtdates = connection.createStatement();
						
//	
//						
						if (pc.next() == false)
						{
							String pinsert = "INSERT INTO PRODUCTS (PRODUCT_ID, PRODUCT_NAME, PRICE) VALUES ('" +  data2.get("PID") + "', '" +  data2.get("PRODUCT_NAME") + "', '" + data2.get("PRICE") + "')";
							stmtp.executeUpdate(pinsert);
						}
						if (cc.next() == false)
						{
							String cinsert = "INSERT INTO CUSTOMERS (CUSTOMER_ID, CUSTOMER_NAME) VALUES ('" +  x.get("CUSTOMER_ID") + "', '" + x.get("CUSTOMER_NAME") + "')";
							stmtc.executeUpdate(cinsert);
						}
						if (stc.next() == false)
						{
							String stinsert = "INSERT INTO STORES (STORE_ID, STORE_NAME) VALUES ('" +  x.get("STORE_ID") + "', '" + x.get("STORE_NAME") + "')";
							stmtst.executeUpdate(stinsert);
						}
						if (supc.next() == false)
						{

							String supinsert = "INSERT INTO SUPPLIERS (SUPPLIER_ID, SUPPLIER_NAME) VALUES ('" +  supid + "', '" + new_supname + "')";
							stmtsup.executeUpdate(supinsert);
						}
						if (datesc.next() == false)
						{
							int quart = 0;
							if (Float.parseFloat(d[1]) <= 3)
							{
								quart = 1;
							}
							else if (Float.parseFloat(d[1]) > 3 & Float.parseFloat(d[1]) <= 6)
							{
								quart = 2;
							}
							else if (Float.parseFloat(d[1]) > 6 & Float.parseFloat(d[1]) <= 9)
							{
								quart = 3;
							}
							else if (Float.parseFloat(d[1]) > 9 & Float.parseFloat(d[1]) <= 12)
							{
								quart = 4;
							}
							String dateinsert = "INSERT INTO DATES (Date_, D, M, QUART, Y) VALUES ('" + dates + "', '" + d[2] + "', '" + d[1] + "', '" + String.valueOf(quart) + "','" + d[0] + "')";
							stmtdates.executeUpdate(dateinsert);
						}
						
						
						float sales_sum = quan*pr;
						
						String sum = String.format("%.02f", sales_sum);

						int quar = 0;
						if (Float.parseFloat(d[1]) <= 3)
						{
							quar = 1;
						}
						else if (Float.parseFloat(d[1]) > 3 & Float.parseFloat(d[1]) <= 6)
						{
							quar = 2;
						}
						else if (Float.parseFloat(d[1]) > 6 & Float.parseFloat(d[1]) <= 9)
						{
							quar = 3;
						}
						else if (Float.parseFloat(d[1]) > 9 & Float.parseFloat(d[1]) <= 12)
						{
							quar = 4;
						}
						String tinsert = "INSERT INTO FACT (TRANSACTION_ID, PRODUCT_ID, QUANTITY, CUSTOMER_ID, STORE_ID, SUPPLIER_ID, DATE_ , D, M, QUART, Y, SALE_MONEY) VALUES ('" + x.get("TRANSACTION_ID") + "', '" + x.get("PID") + "', '" + x.get("QUANTITY") + "', '" + x.get("CUSTOMER_ID") + "', '" + x.get("STORE_ID") + "', '" + data2.get("SUPPLIER_ID") + "', '" + dates + "','" + d[2] + "', '" + d[1] + "', '" + String.valueOf(quar) + "', '" + d[0] + "', '" + sum + "')";
						//System.out.print(tinsert + "\n");
						stmt2.executeUpdate(tinsert);
						counter++;
					}
					
				}
				
				
				
				tlimit1+=50;
				
				limit1+=10;
			
			
			}
			System.out.print("transactions joined : " + counter + "\n");
		
	}
		
	
	
	public static void main(String[] args) throws SQLException {
		DDL();
		sqlConnection();
	}
}
