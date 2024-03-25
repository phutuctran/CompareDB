using System;
using System.Data.Common;
using System.Data;
using Microsoft.Data.SqlClient;
using Microsoft.Identity.Client;
namespace CompareDatabase
{
    public interface ILogger
    {
        void Log(string message);
    }
    public class ConsoleLogger : ILogger
    {
        public void Log(string message)
        {
            Console.WriteLine(message);
        }
    }

    public class DatabaseCompare
    {
        private readonly ILogger logger;

        private List<(string, List<string>)> data1;
        private List<(string, List<string>)> data2;
        private string connectionString1;
        private string connectionString2;
        private bool is_Connected;
        private string nameDB1 = "";
        private string nameDB2 = "";

        public string NameDB2
        {
            get { return nameDB2; }
            set { nameDB2 = value; }
        }

        public string NameDB1
        {
            get { return nameDB1; }
            set { nameDB1 = value; }
        }

        public bool Is_Connected
        {
            get { return is_Connected; }
        }

        public string ConnectionString2
        {
            get { return connectionString2; }
            set { connectionString2 = value; }
        }

        public string ConnectionString1
        {
            get { return connectionString1; }
            set { connectionString1 = value; }
        }

        private static SqlConnection connection1; //single ton
        private static SqlConnection connection2; //single ton

        public DatabaseCompare() { }

        public DatabaseCompare(string _connectionString1, string _connectionString2)
        {
            logger = new ConsoleLogger();
            connectionString1 = _connectionString1;
            connectionString2 = _connectionString2;
            
            connection1 = new SqlConnection(connectionString1);
            connection2 = new SqlConnection(connectionString2);
            is_Connected = ConnectDB(ref connection1, nameDB1) && ConnectDB(ref connection2, nameDB2);
            
        }

        public void Start()
        {
            if (!is_Connected)
            {
                logger.Log("Database is not connected!");
                return;
            }
            data1 = GetALlTableAndColumn(connection1, nameDB1);
            data2 = GetALlTableAndColumn(connection2, nameDB2);
            Compare();
        }

        private bool ConnectDB(ref SqlConnection connection, string name)
        {
            try
            {
                connection.Open();
                logger.Log($"Connected to the database {name}.");
                return true;
            }
            catch (SqlException ex)
            {
                logger.Log($"Error Database {name}: {ex.Message}");
                return false;
            }
        }

        //Lấy tất cả hàng và cột trong DB
        private List<(string, List<string>)> GetALlTableAndColumn(SqlConnection connection, string name)
        {
            DataTable schema = connection.GetSchema("Tables");
            List<(string, List<string>)> TableData = new List<(string, List<string>)>();
            foreach (DataRow row in schema.Rows)
            {
                string tableName = row[2].ToString();
                string[] restrictions = new string[4] { null, null, $"{tableName}", null };
                var columnList = connection.GetSchema("Columns", restrictions).AsEnumerable().Select(s => s.Field<String>("Column_Name")).ToList();
                TableData.Add((row[2].ToString(), columnList));
            }

            //Sort
            TableData.Sort((x, y) => x.Item1.CompareTo(y.Item1));
            foreach(var tmp in TableData)
            {
                tmp.Item2.Sort();
            }

            logger.Log($"\nDatabase {name}: ");
            foreach (var tmp in TableData)
            {
                logger.Log($"* {tmp.Item1}");
                foreach(var column in tmp.Item2)
                {
                    logger.Log($"   |--{column}");
                }
            }
            return TableData;
            
        }

        //So sánh db1 với db2
        private void Compare()
        {

            //Kiểm tra table có bi xóa không, nếu ko bị xóa thì kiểm tra sự thay đổi column
            foreach (var table in data1)
            {
                string name1 = table.Item1;
                int index = data2.FindIndex(a => a.Item1 == name1);
                if (index < 0)
                {
                    logger.Log($"Table [{name1}] was deleted!");
                }
                else
                {
                    //Kiểm tra column có bị xóa ko.
                    foreach (string column in table.Item2)
                    {
                        int indexColumn = data2[index].Item2.FindIndex(a => a == column);
                        if (indexColumn < 0)
                        {
                            logger.Log($"Column ({column}) in [{name1}] was deleted!");
                        }
                    }
                    //Kiểm tra có column nào mới đc tạo ko
                    foreach(string column in data2[index].Item2)
                    {
                        int indexColumn = table.Item2.FindIndex(a => a == column);
                        if (indexColumn < 0)
                        {
                            logger.Log($"Column ({column}) in [{name1}] was created!");
                        }
                    }    
                }
            }

            //Kiểm tra tạo table mới
            foreach (var table in data2)
            {
                string name2 = table.Item1;
                int index = data1.FindIndex(a => a.Item1 == name2);
                if (index < 0)
                {
                    logger.Log($"Table [{name2}] was created!");
                }
            }

        }
    }
    internal class Program
    {
        static void Main(string[] args)
        {

            //database 1 là mới sẽ được so sánh với database 2 (cũ)
            string connectionStr2 = "data source=.\\SQLEXPRESS;initial catalog=WatchStore;integrated security=True;MultipleActiveResultSets=True; TrustServerCertificate=True";
            string connectionStr1 = "data source=.\\SQLEXPRESS;initial catalog=WatchStore2;integrated security=True;MultipleActiveResultSets=True; TrustServerCertificate=True";
            var DBcompare = new DatabaseCompare(connectionStr1, connectionStr2);
            DBcompare.NameDB1 = "1";
            DBcompare.NameDB2 = "2";

            DBcompare.Start();

        }
    }


   
}
