using System;
using System.Data.Common;
using System.Data;
using Microsoft.Data.SqlClient;
using Microsoft.Identity.Client;
using System.Reflection.PortableExecutable;
using System.Text;
using CompareDatabase;
namespace CompareDatabase
{
    public struct ColumnInfo
    {
        public ColumnInfo()
        {
            name = "";
            dataType = "";
            charMaxLength = 0;
            isNullable = true;
            isPK = false;
        }

        public ColumnInfo(string _name, string _dataType, bool _isNullable, bool _isPK, int _charMaxLength)
        {
            name = _name;
            dataType = _dataType;
            isNullable = _isNullable;
            isPK = _isPK;
            charMaxLength = _charMaxLength;
        }

        public string name;
        public string dataType;
        public int? charMaxLength;
        public bool isNullable;
        public bool isPK;
    }


    public interface ILogger
    {
        void Log(string message, string name = "");
    }
    public class Logger : ILogger
    {
        private string filePath;

        public Logger(string path)
        {
            filePath = path;
        }
        public void Log(string message, string name = "")
        {
            Console.WriteLine(name + "--> " + message);
            using (StreamWriter writer = new StreamWriter(filePath, true))
            {
                writer.WriteLine(message);
            }
        }
    }

    public class DatabaseCompare
    {
        private readonly ILogger logger;
        private readonly ILogger setFileDB1;
        private readonly ILogger setFileDB2;

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
            string logFile = "C:\\PUBLISH\\compareDatabase\\Logs\\" + DateTime.Now.ToString().Replace('/', '_').Replace(':', '_') + ".txt";
            string settingFile1 = "C:\\PUBLISH\\compareDatabase\\AdjustDB\\SettingFileDB1.txt";
            string settingFile2 = "C:\\PUBLISH\\compareDatabase\\AdjustDB\\SettingFileDB2.txt";
            logger = new Logger(logFile);
            setFileDB1 = new Logger(settingFile1);
            setFileDB2 = new Logger(settingFile2);
            DeleteFile(settingFile1);
            DeleteFile(settingFile2);

            connectionString1 = _connectionString1;
            connectionString2 = _connectionString2;

            connection1 = new SqlConnection(connectionString1);
            connection2 = new SqlConnection(connectionString2);
            is_Connected = ConnectDB(ref connection1) && ConnectDB(ref connection2);
        }

        private string GetDatabaseName(SqlConnection connection)
        {
            return connection.Database;
        }

        private void DeleteFile(string filePath)
        {
            if (File.Exists(filePath))
            {
                // Xóa tập tin
                File.Delete(filePath);
            }
        }

        public void Start()
        {
            if (!is_Connected)
            {
                logger.Log("Database is not connected!");
                return;
            }
            nameDB1 = GetDatabaseName(connection1);
            nameDB2 = GetDatabaseName(connection2);
            data1 = GetAllTableAndColumn(connection1, nameDB1);
            data2 = GetAllTableAndColumn(connection2, nameDB2);
            setFileDB1.Log($"USE {nameDB1}");
            setFileDB2.Log($"USE {nameDB2}");
            Compare();
        }

        private bool ConnectDB(ref SqlConnection connection)
        {
            try
            {
                connection.Open();

                logger.Log($"Connected to the database {connection.Database}.");
                return true;
            }
            catch (SqlException ex)
            {
                logger.Log($"Error Database {connection.Database}: {ex.Message}");
                return false;
            }
        }

        //Lấy tất cả hàng và cột trong DB
        private List<(string, List<string>)> GetAllTableAndColumn(SqlConnection connection, string name)
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
            foreach (var tmp in TableData)
            {
                tmp.Item2.Sort();
            }

            //logger.Log($"\nDatabase {name}: ");
            //foreach (var tmp in TableData)
            //{
            //    logger.Log($"* {tmp.Item1}");
            //    foreach (var column in tmp.Item2)
            //    {
            //        logger.Log($"   |--{column}");
            //    }
            //}
            return TableData;

        }

        //So sánh db1 với db2
        private void Compare()
        {

            //Kiểm tra table có đc tạo không
            foreach (var table in data1)
            {
                string name1 = table.Item1;
                int index = data2.FindIndex(a => a.Item1 == name1);
                if (index < 0)
                {
                    logger.Log($"Table [{name1}] in Database {nameDB1} was created!", "log");
                    //DB1 có nhưng DB2 không có
                    //Tạo mới bảng này ở DB2
                    //setFileDB2.Log($"C [{name1}]");
                    var script = $"CREATE TABLE [{name1}] ( tmp int);";
                    setFileDB2.Log(script, $"{nameDB1}");
                    //List<ColumnInfo> columnInfos = new List<ColumnInfo>();

                    foreach (string column in table.Item2)
                    {
                        var columnInfo = GetDataTypeColumn(connection1, name1, column);
                        logger.Log($"Column ({column}) in [{name1}] in Database {nameDB1} was created! : {columnInfo.dataType} {columnInfo.charMaxLength}, isNullable: {columnInfo.isNullable}, isPK: {columnInfo.isPK}", "log");
                        //columnInfos.Add(columnInfo);
                        script = AddColumn(name1, columnInfo);
                        setFileDB2.Log(script, $"{nameDB1}");
                    }
                    script = $"ALTER TABLE [{name1}] DROP COLUMN tmp;";
                    setFileDB2.Log(script, $"{nameDB1}");
                }
                else
                {
                    //Kiểm tra column có bị xóa ko.
                    foreach (string column in table.Item2)
                    {
                        int indexColumn = data2[index].Item2.FindIndex(a => a == column);
                        if (indexColumn < 0)
                        {
                            var columnInfo = GetDataTypeColumn(connection1, table.Item1, column);
                            logger.Log($"Column ({column}) in [{name1}] in Database {nameDB1} was created! : {columnInfo.dataType} {columnInfo.charMaxLength}, isNullable: {columnInfo.isNullable}, isPK: {columnInfo.isPK}", "log");

                            string script = AddColumn(name1, columnInfo);
                            setFileDB2.Log(script, $"{nameDB1}");
                        }
                    }
                    //Kiểm tra có column nào mới đc tạo ko
                    foreach (string column in data2[index].Item2)
                    {
                        int indexColumn = table.Item2.FindIndex(a => a == column);
                        if (indexColumn < 0)
                        {
                            logger.Log($"Column ({column}) in [{name1}] was deleted!", "log");
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
                    logger.Log($"Table [{name2}] was deleted!", "log");
                }
            }
        }
        private string AddColumn(string tabelName, ColumnInfo columnInfo)
        {
            string script = $"ALTER TABLE [{tabelName}] ADD {columnInfo.name} {columnInfo.dataType}" + (columnInfo.charMaxLength != null ? string.Format("(" + (columnInfo.charMaxLength == -1 ? "MAX" : columnInfo.charMaxLength) + ")") : "") + (columnInfo.isNullable == true ? "" : " not null") + ";";
            
            if (columnInfo.isPK == true) 
            {
               script += $"\nALTER TABLE [{tabelName}] ADD CONSTRAINT PK_{Guid.NewGuid().ToString().Replace("-", "")} PRIMARY KEY ({columnInfo.name});";
            }
            return script;
        }

        private ColumnInfo GetDataTypeColumn(SqlConnection connection, string tableName, string columnName)
        {
            var column = new ColumnInfo();
            column.name = columnName;

            string query = $"SELECT DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{tableName}' AND COLUMN_NAME = '{columnName}'";

            string query2 = $@"
            SELECT COUNT(*)
            FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
            WHERE OBJECTPROPERTY(OBJECT_ID(CONSTRAINT_SCHEMA + '.' + CONSTRAINT_NAME), 'IsPrimaryKey') = 1
            AND TABLE_NAME = '{tableName}'
            AND COLUMN_NAME = '{columnName}'";

            using (SqlCommand command = new SqlCommand(query, connection))
            {
                SqlDataReader result = command.ExecuteReader();
                if (result.HasRows)
                {
                    result.Read();
                    column.dataType = result.GetString(0);
                    column.charMaxLength = result.IsDBNull(1) ? null : (int?)result.GetInt32(1);
                    column.isNullable = result.GetString(2) == "YES";
                }
            }

            using (SqlCommand command = new SqlCommand(query2, connection))
            {
                int count = (int)command.ExecuteScalar();
                column.isPK = count > 0;

            }

            return column;

        }
        internal class Program
        {
            static void Main(string[] args)
            {
                //database 1 là mới sẽ được so sánh với database 2 (cũ)
                string connectionStr1 = "data source=.\\SQLEXPRESS;initial catalog=WatchStore;integrated security=True;MultipleActiveResultSets=True; TrustServerCertificate=True";
                string connectionStr2 = "data source=.\\SQLEXPRESS;initial catalog=WatchStore2;integrated security=True;MultipleActiveResultSets=True; TrustServerCertificate=True";
                var DBcompare = new DatabaseCompare(connectionStr1, connectionStr2);
                //DBcompare.NameDB1 = "1";
                //DBcompare.NameDB2 = "2";

                DBcompare.Start();

            }
        }



    }
}