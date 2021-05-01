package incubyte;

import java.io.*;
import java.sql.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Iterator;

class Customer{
String Customer_Name;
String Customer_ID;
String Customer_Open_Date;
String Last_Consulted_Date;
String Vaccination_Type;
String Doctor_Consulted;
String State;
String Country;
int Post_Code;
String Date_of_Birth;
String Active_Customer;
}

public class Incubyte {

static Connection conn1 = null;
static Statement stmt = null;


    public static void main(String[] args) throws Exception {
        Incubyte i = new Incubyte();
        boolean output = i.readFile_prepareStageTable();
        if(output == true)
            System.out.println("Stage table prepared");
        output = i.Stg_to_CountryWise();
        if(output == true)
            System.out.println("Country wise table prepared");
    }
    
    public static void establish_Connection() throws Exception {
        conn1 = DriverManager.getConnection("jdbc:mysql://localhost:3306/mydatabase","root","");
        stmt = conn1.createStatement();    
    }
    
    public boolean readFile_prepareStageTable() {
        try{
            File file = new File("C:\\Users\\Onkar\\OneDrive\\Desktop\\20210501_165300.txt");
            FileReader fileReader = new FileReader(file);
            BufferedReader br = new BufferedReader(fileReader);
            String line = "";
            String column_list[] = null;
            String data[]=null;
            Customer c =  new Customer();
            while((line=br.readLine())!=null)
            {
                if(line.charAt(1)=='H' || line.charAt(1)=='h')
                {
                    line = line.substring(3);
                    column_list = line.split("\\|");
                    continue;
                }
                else if(line.charAt(1)=='D' || line.charAt(1)=='D')
                {
                    line = line.substring(3);
                    data = line.split("\\|");
                }
                else
                {
                    System.out.println("Error Occurred in line:\n" + line);
                }
                for(int i= 0;i<data.length;i++)
                {
                   if(column_list[i].equals("Customer_Name"))
                   {
                    c.Customer_Name = data[i];   
                   }
                   else if(column_list[i].equals("Customer_Id"))
                   {
                    c.Customer_ID = data[i];   
                   }
                   else if(column_list[i].equals("Open_Date"))
                   {
                       c.Customer_Open_Date = data[i];
                   }
                   else if(column_list[i].equals("Last_Consulted_Date"))
                   {
                       c.Last_Consulted_Date = data[i];
                   }
                   else if(column_list[i].equals("Vaccination_Id"))
                   {
                       c.Vaccination_Type = data[i];
                   }
                   else if(column_list[i].equals("Dr_Name"))
                   {
                       c.Doctor_Consulted = data[i];
                   }
                   else if(column_list[i].equals("State"))
                   {
                       c.State = data[i];
                   }
                   else if(column_list[i].equals("Country"))
                   {
                       c.Country = data[i];
                   }
                   else if(column_list[i].equals("DOB"))
                   {
                       c.Date_of_Birth = data[i];
                   }
                   else if(column_list[i].equals("Is_Active"))
                   {
                       c.Active_Customer = data[i];
                   }
                   else if(column_list[i].equals("Post_Code"))
                   {
                       c.Post_Code = Integer.parseInt(data[i]);
                   }

                }
                Insert_to_Staging_Table(c);   
            }
        }catch(Exception e)
        {
            e.printStackTrace();
        }
        return true;
    }
    
    public boolean Insert_to_Staging_Table(Customer c) throws Exception {
        try
        {
            Incubyte.establish_Connection();
            
            String query = " insert into Cust_Stage (Customer_Name, Customer_ID,Customer_Open_Date, Last_Consulted_Date,Vaccination_Type,Doctor_Consulted,"
                + "State,Country,Post_Code,Date_of_Birth,Active_Customer)"
                + " values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
            
            Customer c1 = c;
            
            PreparedStatement pStatement = conn1.prepareStatement(query);
            pStatement.setString(1, c1.Customer_Name);
            pStatement.setString(2, c1.Customer_ID);
            pStatement.setString(3,c1.Customer_Open_Date);
            pStatement.setString(4,c1.Last_Consulted_Date);
            pStatement.setString(5, c1.Vaccination_Type);
            pStatement.setString(6, c1.Doctor_Consulted);
            pStatement.setString(7, c1.State);
            pStatement.setString(8, c1.Country);
            pStatement.setInt(9,c1.Post_Code);
            pStatement.setString(10,c1.Date_of_Birth.substring(4) +""+ c1.Date_of_Birth.substring(0,2) +""+ c1.Date_of_Birth.substring(2,4));
            pStatement.setString(11, c1.Active_Customer);

            pStatement.execute();
            
            Statement st = conn1.createStatement();
            st.execute("update cust_stage set Post_Code = null where Post_code=0");
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }
        finally
        {
            conn1.close();
            stmt.close();
        }
        return true;
    }
        
    public boolean Stg_to_CountryWise() throws Exception
    {
        try
        {
            Incubyte.establish_Connection();
            java.util.ArrayList<String> Country_List = new java.util.ArrayList<String>();
        
            ResultSet rs = stmt.executeQuery("select Country from cust_stage");
            
            while(rs.next())
            {
                Country_List.add(rs.getString("Country"));
            }

            Iterator<String> itr = Country_List.iterator();
            while(itr.hasNext())
            {
                String country = itr.next();
                rs = stmt.executeQuery("select * from information_schema.tables ");
                boolean tExist=false;
                while(rs.next())
                {
                    if(rs.getString("TABLE_NAME").toString().equalsIgnoreCase("Table_" + country))
                    {
                        tExist = true;
                        break;
                    }
                }
                if(tExist==false)
                {
                    String create_table = "create table Table_"+country +"(Name varchar(255) not null," + "Cust_I varchar(18) not null, " +
                    "Open_Dt date not null," +
                    "Consul_Dt date," +
                    "VAC_ID char(5)," +
                    "DR_Name char(255)," +
                    "State char(5)," +
                    "Country char(5)," +
                    "Post_Code int(5)," +
                    "DOB date," +
                    "FLAG char(1))";

                    stmt.execute(create_table);
                }                
                String Insert_Query = "insert into Table_" + country + " select * from cust_stage where Country = '" + country + "'";
                stmt.execute(Insert_Query);
            }   
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }
        finally
        {
            conn1.close();
            stmt.close();
        } 
        return true;
    }
}
