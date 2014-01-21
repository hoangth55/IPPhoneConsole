using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.Data.SqlClient;
using System.Windows;


namespace IPPhoneConsole
{
    public class Program
    {
        private SqlConnection con;
        private SqlDataAdapter da = new SqlDataAdapter();

        private DataTable dtPartition = new DataTable("Partition");
        private DataTable dtContactPhone = new DataTable("ContactPhone");
        private DataTable dtPriceList = new DataTable("PriceList");

        private List<NumberInternational> NumberInternationalTable = new List<NumberInternational>();
        private int totalNumberInternaltional = 0;

        static void Main(string[] args)
        {
            Program program = new Program();
            program.pushCallRecord();

            Console.ReadLine();
        }

        //-----------------------Establish onnection to DB--------------------------
        private void connect()
        {
            String cn = "Data Source=ADVENTURE\\HUYHOANG; Initial Catalog = IPPhone; Persist Security Info = True; User ID = sa; Password = 123456";
            //String cn = "Data Source = (local); Initial Catalog = iPMAC; Integrated Security = True";
            try
            {
                con = new SqlConnection(cn);
                con.Open(); //Mo cket noi
                //MessageBox.Show("Successful!", "Connected DB Successful!", MessageBoxButtons.OK, MessageBoxIcon.Information);

            }
            catch (Exception ex)
            {
                System.Console.WriteLine(ex.Message + ". Khong the ket noi toi DB!");

            }
        }
        //------------------------Finish connection to DB-----------------------

        //------------------------Destroy connection to DB-------------------------
        private void disconnect()
        {
            con.Close();
            con.Dispose();
            con = null;
        }
        //------------------------Finish destroy connection to DB---------------

        //----------------------------Create Contact tablie from Database------------------
        public List<ContactPhone> createContactTable()
        {
            SqlCommand cmdContact = new SqlCommand();
            cmdContact.Connection = con;
            cmdContact.CommandType = CommandType.Text;
            cmdContact.CommandText = @"Select   Contact.DirectoryNumber as N'DirectoryNumber',
                                                        Owner as N'Owner',
                                                        Department as N'Department',
                                                        Company as N'Company'
                                                    from Contact";

            da.SelectCommand = cmdContact;
            dtContactPhone.Clear();
            da.Fill(dtContactPhone);

            int rowContact = dtContactPhone.Rows.Count;
            DataRow[] dtrowContact = dtContactPhone.Select();

            List<ContactPhone> ContactTable = new List<ContactPhone>();

            for (int i = 0; i < rowContact; i++)
            {
                ContactPhone contactRecord = new ContactPhone();
                contactRecord.DirectoryNumber = Convert.ToInt32(dtrowContact[i]["DirectoryNumber"].ToString());
                contactRecord.Owner = dtrowContact[i]["Owner"].ToString();
                contactRecord.Department = dtrowContact[i]["Department"].ToString();
                contactRecord.Company = dtrowContact[i]["Company"].ToString();
                ContactTable.Add(contactRecord);
            }

            return ContactTable;
        }
        //-----------------------------Finish getting Contact Table from DB---------------------

        //----------------------------Create Price List tablie from Database------------------
        public List<PriceList> createPriceListTable()
        {
            SqlCommand cmdPriceList = new SqlCommand();
            cmdPriceList.Connection = con;
            cmdPriceList.CommandType = CommandType.Text;
            cmdPriceList.CommandText = @"Select   PriceList.ID as N'ID',
                                                        Category as N'Category',
                                                        NumberHeader as N'NumberHeader',
                                                        Minute as N'Minute',
                                                        Block6 as N'Block6',
                                                        Second as N'Second',
                                                        Type as N'Type'
                                                    from PriceList";

            da.SelectCommand = cmdPriceList;
            dtPriceList.Clear();
            da.Fill(dtPriceList);

            int rowPriceList = dtPriceList.Rows.Count;
            DataRow[] dtrowPriceList = dtPriceList.Select();

            List<PriceList> PriceListTable = new List<PriceList>();



            for (int i = 0; i < rowPriceList; i++)
            {
                PriceList priceListTable = new PriceList();
                priceListTable.Category = dtrowPriceList[i]["Category"].ToString();
                priceListTable.Minute = dtrowPriceList[i]["Minute"].ToString();
                priceListTable.Block6 = dtrowPriceList[i]["Block6"].ToString();
                priceListTable.Second = dtrowPriceList[i]["Second"].ToString();
                priceListTable.Type = dtrowPriceList[i]["Type"].ToString();
                PriceListTable.Add(priceListTable);

                if (dtrowPriceList[i]["Type"].ToString() == "2")
                {
                    NumberInternational numberInternaltional = new NumberInternational();
                    numberInternaltional.Number = dtrowPriceList[i]["NumberHeader"].ToString();
                    numberInternaltional.Minute = dtrowPriceList[i]["Minute"].ToString();

                    NumberInternationalTable.Add(numberInternaltional);
                }
            }
            totalNumberInternaltional = NumberInternationalTable.Count;

            //------------Sort NumberHeader in NumberInternational by Length of Header---------------
            
            //-----------Finish sorting------------

            return PriceListTable;
        }
        //-----------------------------Finish getting PriceList Table from DB---------------------


        //----------------------------Computing Charging for each Call records----------------------
        public double computeCharging(  int type_call, int timeDuration, 
                                        double priceMinute, double priceBlock6, double priceSecond)
        {
            double totalCharging = 0;

            switch (type_call)
            {
                case 0://For "longDistance" and "Mobile" call records
                    int _second = 0;
                    if (timeDuration > 6)
                        _second = timeDuration - 6;

                    totalCharging = priceBlock6 + _second * priceSecond;
                    break;
                case 1://For "Local" and "Service" call records
                    int _minute = timeDuration / 60;
                    if (timeDuration % 60 > 0)
                        _minute++;
                    else { }

                    totalCharging = _minute * priceMinute;
                    break;

                case 2://For "International" call records
                    /*/----------Notice............
                    bool found = false;
                    for (int i = 0; i < dtPartition.Rows.Count; i++)
                    {
                        if (PriceListTable[PriceListIndex].NumberHeader.IndexOf(finalCalledPartyNumber) != -1)
                        {
                            totalCharging = _minute * Convert.ToDouble(dtrow3[i]["Minute"].ToString());
                            found = true;
                            break;
                        }

                    }
                    List<NumberInternational> test;
                    if (!found)
                        for (int i = 0; i < dtPartition.Rows.Count; i++)
                            if (dtrow3[i]["NumberHeader"].ToString() == "0")
                                totalCharging = _minute * Convert.ToDouble(dtrow3[i]["Minute"].ToString());
                    continue;
                   */
                    {

                    }
                    break;

                default:
                    break;
            }

            return totalCharging;
        }
        //---------------------------------------Finish to compute charging----------------------

        //-----------------------------Push each Call Record to DB------------------------------
        private void pushCallRecordToDatabase( int callingPartyNumber, string finalCalledPartyNumber, DateTime dateTimeConnect,
                                    DateTime dateTimeDisconnect, string finalCalledPartyNumberPartition, string duration, 
                                    double totalCharging, string authCodeDescription)
        {
            string sql = "INSERT INTO CallRecord(CallingPartyNumber, AuthCodeDescription,FinalCalledPartyNumber, DateTimeConnect, DateTimeDisconnect, FinalCalledPartyNumberPartition, Duration, TotalCharging) values (@CallingPartyNumber_, @AuthCodeDescription_,@FinalCalledPartyNumber_, @DateTimeConnect_,@DateTimeDisconnect_, @FinalCalledPartyNumberPartition_, @Duration_, @TotalCharging_)";
            //con.Open();
            SqlCommand cmd = new SqlCommand();
            cmd.Connection = con;
            cmd.CommandType = CommandType.Text;
            cmd.CommandText = sql;
            cmd.Parameters.AddWithValue("@CallingPartyNumber_", callingPartyNumber);
            cmd.Parameters.AddWithValue("@FinalCalledPartyNumber_", finalCalledPartyNumber);
            cmd.Parameters.AddWithValue("@DateTimeConnect_", dateTimeConnect);
            cmd.Parameters.AddWithValue("@DateTimeDisconnect_", dateTimeDisconnect);
            cmd.Parameters.AddWithValue("@FinalCalledPartyNumberPartition_", finalCalledPartyNumberPartition);
            cmd.Parameters.AddWithValue("@Duration_", duration);
            cmd.Parameters.AddWithValue("@TotalCharging_", totalCharging);
            cmd.Parameters.AddWithValue("@AuthCodeDescription_", authCodeDescription);
            try
            {
                //con.Open();
                int recordsAffected = cmd.ExecuteNonQuery();
            }
            catch (System.Data.SqlClient.SqlException sqlException)
            {
                System.Console.WriteLine(sqlException.Message);
            }
        }
        //-----------------------------Finish to push each Call Record to DB---------------------

        //-----------------------------Push all Call Records to DB-----------------------
        private void pushCallRecord()
        {
            connect();

            string full_path = System.IO.Path.GetFullPath("C:\\Users\\Huy Hoang\\Desktop\\IP Phone\\thang12.csv");

            try
            {
                System.IO.StreamReader myFile = new System.IO.StreamReader(full_path);

                //--------Push to database---------------------

                string myString = myFile.ReadLine();

                string[] split = myString.Split(new Char[] { ',' });

                int callingPartyNumberIndex = Array.IndexOf(split, "callingPartyNumber");
                //Console.WriteLine(callingPartyNumberIndex);

                int finalCalledPartyNumberIndex = Array.IndexOf(split, "finalCalledPartyNumber");
                //Console.WriteLine(finalCalledPartyNumberIndex);

                int dateTimeConnectIndex = Array.IndexOf(split, "dateTimeConnect");
                // Console.WriteLine(dateTimeConnectIndex);

                int dateTimeDisconnectIndex = Array.IndexOf(split, "dateTimeDisconnect");
                //Console.WriteLine(dateTimeDisconnectIndex);

                int finalCalledPartyNumberPartitionIndex = Array.IndexOf(split, "finalCalledPartyNumberPartition");
                // Console.WriteLine(finalCalledPartyNumberPartitionIndex);

                int durationIndex = Array.IndexOf(split, "duration");
                //Console.WriteLine(durationIndex);
                int authCodeDescriptionIndex = Array.IndexOf(split, "authCodeDescription");
                //int count = 0;

                //-------------Create table ContactPhone from Database--------------

                List<ContactPhone> ContactTable = createContactTable();
                int totalContactTable = ContactTable.Count;


                //-------------Finish get ContactPhone------------

                //-------------Create table 'PriceList' and table 'Number International' from Database--------------
                List<PriceList> PriceListTable = createPriceListTable();

                int totalPriceListTable = PriceListTable.Count;

                //-------------Finish get PriceList- and table Number International-----------

                //-------------Read all file cdr to push to DB--------------------------
                while (!myFile.EndOfStream)
                {

                    string record = myFile.ReadLine();

                    string[] fields = record.Split(new Char[] { ',' });

                    int callingPartyNumber = Convert.ToInt32(fields[callingPartyNumberIndex]);
                    string finalCalledPartyNumber = fields[finalCalledPartyNumberIndex];
                    DateTime dateTimeConnect = FromUnixTime(Convert.ToDouble(fields[dateTimeConnectIndex]));
                    DateTime dateTimeDisconnect = FromUnixTime(Convert.ToDouble(fields[dateTimeDisconnectIndex]));
                    string finalCalledPartyNumberPartition = fields[finalCalledPartyNumberPartitionIndex];
                    string duration = fields[durationIndex];
                    string authCodeDescription = fields[authCodeDescriptionIndex];

                    double totalCharging = 0;


                    //--------------Check to get only Calling with Duration > 0 -----------------
                    if (Convert.ToInt32(duration) > 0)
                    {
                        //---------------------Info Number-------------------
                        bool ContactFound = false;
                        int ContactMatch = 0;
                        for (int contactIndex = 0; contactIndex < totalContactTable; contactIndex++)
                        {
                            if (ContactTable[contactIndex].DirectoryNumber == callingPartyNumber)
                            {
                                ContactMatch = contactIndex;
                                ContactFound = true;
                                break;
                            }
                        }

                        //-------------Checking to match Contact---------
                        if (ContactFound)
                        {
                            //callingPartyNumber = Convert.ToString(ContactTable[ContactIndex].DirectoryNumber);
                            //-------------------Finish get calling ID--------------------
                            
                            //-------------------Checking to get Full Information of Calling: authCodeDescription
                            if (authCodeDescription == "")
                            {
                                authCodeDescription = ContactTable[ContactMatch].Department;
                            } //-------------------Finish to get full Information

                            
                            bool PriceListFound = false;
                            int PriceListMatch = 0;
                            for (int priceListIndex = 0; priceListIndex < totalPriceListTable; priceListIndex++)
                            {
                                if (PriceListTable[priceListIndex].Category == finalCalledPartyNumberPartition)
                                {
                                    PriceListMatch = priceListIndex;
                                    PriceListFound = true;
                                    break;
                                }
                            }

                            if (PriceListFound)
                            {
                                int type_call = Convert.ToInt32(PriceListTable[PriceListMatch].Type);
                                int timeDuration = Convert.ToInt32(duration);
                                double priceMinute = Convert.ToDouble(PriceListTable[PriceListMatch].Minute);
                                double priceBlock6 = Convert.ToDouble(PriceListTable[PriceListMatch].Block6);
                                double priceSecond = Convert.ToDouble(PriceListTable[PriceListMatch].Second);
                                
                                totalCharging = computeCharging(type_call, timeDuration, priceMinute, priceBlock6, priceSecond);


                            }
                            else //--------If not found in PriceList Info, it will return 0;
                            {
                                totalCharging = 0;
                            }
                            //--------- Finish compute charging---------------

                            pushCallRecordToDatabase(callingPartyNumber, finalCalledPartyNumber, dateTimeConnect,
                                    dateTimeDisconnect, finalCalledPartyNumberPartition, duration,
                                    totalCharging, authCodeDescription);

                            
                            //-----------Push each call to server--------------
                           // count++;
                            
                        }
                        else
                        {
                            continue;
                        }//----------------------Finish to checking Info Number--------------- 

                    }//--------------Finish to check Calling with Duration > 0 -----------------

                }
                disconnect();
               
                myFile.Close();
                //System.Console.ReadLine();
                //System.Console.WriteLine("Finish!", "Finish total " + count + "records!");
            }
            catch (Exception ex)
            {
                Console.WriteLine("The file could not be read:");
                Console.WriteLine(ex.Message);
            }
        }
        //-----------------------------Finish to Push all Call Records to DB------


        ////-----------------------------Push all Contacts to DB-------------------
        private void pushContact()
        {
            connect();

            string full_path = System.IO.Path.GetFullPath("C:\\Users\\Huy Hoang\\Desktop\\IP Phone\\");
            try
            {
                System.IO.StreamReader myFile = new System.IO.StreamReader(full_path);

                string myString = myFile.ReadLine();


                string[] split = myString.Split(new Char[] { ',' });

                int directoryNumberIndex = Array.IndexOf(split, "officephone");

                int ownerIndex = Array.IndexOf(split, "samaccountname");

                int departmentIndex = Array.IndexOf(split, "department");

                int companyIndex = Array.IndexOf(split, "company");
      
                int count = 0;
                while (!myFile.EndOfStream)
                {
                    count++;
                    string record = myFile.ReadLine();

                    string[] fields = record.Split(new Char[] { ',' });

                    string DirectoryNumber = fields[directoryNumberIndex];
                    string Owner = fields[ownerIndex];
                    string Company = fields[companyIndex];
                    string Department = fields[departmentIndex];



                    //-----------Push each call to server--------------
                    string sql = "INSERT INTO InfoNumberPhone(DirectoryNumber, Owner, Department, Company) values (@DirectoryNumber_, @Owner_, @Department_, @Company_)";
                    //con.Open();@PhoneNumber_, @Department_Name_, @Department_Des
                    SqlCommand cmd = new SqlCommand();
                    cmd.Connection = con;
                    cmd.CommandType = CommandType.Text;
                    cmd.CommandText = sql;
                    cmd.Parameters.AddWithValue("@DirectoryNumber_", DirectoryNumber);
                    cmd.Parameters.AddWithValue("@Owner_", Owner);
                    cmd.Parameters.AddWithValue("@Department_N", Department);
                    cmd.Parameters.AddWithValue("@Company_", Company);

                    try
                    {
                        //con.Open();
                        int recordsAffected = cmd.ExecuteNonQuery();
                    }
                    catch (System.Data.SqlClient.SqlException sqlException)
                    {
                        System.Console.WriteLine(sqlException.Message);
                    }

                }
                disconnect();

            }
            catch (Exception ex)
            {
                Console.WriteLine("The file could not be read:");
                Console.WriteLine(ex.Message);
            }
        }
        //-----------------------------Finish to Push all Contacts to DB------

        //--------------------------Function to convert time from UNIX to REAL Time-----------------
        public DateTime FromUnixTime(Double unixTime)
        {
            var epoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
            return epoch.AddSeconds(unixTime);
        }
        //-------------------------End of FromUnixTime Function -----------------
    }

    //-----------------------Create table NumberInternaltional from DB------------------
    public class NumberInternational
    {
        private string number;

        public string Number
        {
            get { return number; }
            set { number = value; }
        }
        private string minute;

        public string Minute
        {
            get { return minute; }
            set { minute = value; }
        }

        private List<string> prefix;

        public List<string> Prefix
        {
            get { return prefix; }
            set { prefix = value; }
        }

    }
    //-----------------------End of class NumberInternational------------------------

    //-----------------------Create table Partition Information from DB------------------
    public class PartitionInfo
    {
        private int id;

        public int Id
        {
            get { return id; }
            set { id = value; }
        }
        private string name;

        public string Name
        {
            get { return name; }
            set { name = value; }
        }
    }
    //-----------------------End of class PartitionInformation------------------------

    //-----------------------Create table Contact from DB------------------
    public class ContactPhone
    {
        private int directoryNumber;

        public int DirectoryNumber
        {
            get { return directoryNumber; }
            set { directoryNumber = value; }
        }
        private string owner;

        public string Owner
        {
            get { return owner; }
            set { owner = value; }
        }
        private string department;

        public string Department
        {
            get { return department; }
            set { department = value; }
        }
        private string company;

        public string Company
        {
            get { return company; }
            set { company = value; }
        }



    }
    //-----------------------End of class Contact------------------------

    //-----------------------Create table PriceList from DB------------------
    public class PriceList
    {
        private string category;

        public string Category
        {
            get { return category; }
            set { category = value; }
        }

        private string minute;

        public string Minute
        {
            get { return minute; }
            set { minute = value; }
        }
        private string block6;

        public string Block6
        {
            get { return block6; }
            set { block6 = value; }
        }
        private string second;

        public string Second
        {
            get { return second; }
            set { second = value; }
        }
        private string type;

        public string Type
        {
            get { return type; }
            set { type = value; }
        }
    }
    //-----------------------End of class PriceList------------------------
    
}
