using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace SQLServerToJSON {
    public class Extractor {
        Config _Config = IO.Read();
        SQL _SQL;

        /// <summary>
        /// Initialize the connection to the SQL Server
        /// </summary>
        public Extractor() {
            this._SQL = this._Config.Username == "" ? new SQL(this._Config.Server, this._Config.Database) : new SQL(this._Config.Server, this._Config.Database, this._Config.Username, this._Config.Password);
        }

        /// <summary>
        /// It run the soft.
        /// </summary>
        public void Run() {
            Logger.Write("Starting...");
            foreach (string name in GetTablesName()) {
                WriteDataToJsonFile(name, GetTableColumnNames(name), GetTableData(name));
            }
            Logger.Write("Ending...");
        }

        /// <summary>
        /// Format data to their data type in the JSON file.
        /// </summary>
        /// <param name="datatype">String data type received from SQL Server</param>
        /// <param name="data">String data received from SQL Server</param>
        /// <returns></returns>
        private object FormatData(string datatype, string data) {
            if (datatype == "int") {
                return Convert.ToInt32(data);
            } else if (datatype == "bit") {
                return Convert.ToBoolean(data);
            } else if (datatype == "datetime" || datatype == "datetime2") {
                return DateTime.Parse(data).ToString();
            } else {
                return data;
            }
        }

        /// <summary>
        /// Write the raw data received from SQL Server to a JSON file. The file will have the name of the data table.
        /// </summary>
        /// <param name="tableName">SQL data table. (It will be the name of the file)</param>
        /// <param name="columns">List of the SQL data table columns.</param>
        /// <param name="dataRows">List of SQL data rows.</param>
        private void WriteDataToJsonFile(string tableName, List<string[]> columns, List<List<string>> dataRows) {
            Logger.Write("Writing JSON file for " + tableName + "...");
            List<Dictionary<string, object>> jsonData = new List<Dictionary<string, object>>();
            foreach (var row in dataRows) {
                Dictionary<string, object> jsonObj = new Dictionary<string, object>();
                for (int i = 0; i < row.Count; ++i) {
                    jsonObj.Add(columns[i][0], FormatData(columns[i][1], row[i]));
                }

                jsonData.Add(jsonObj);
            }

            if (!Directory.Exists(".\\output")) {
                Directory.CreateDirectory(".\\output");
            }

            File.AppendAllText(".\\output\\" + tableName + ".json", JsonConvert.SerializeObject(jsonData), Encoding.UTF8);
            Logger.Write(".\\output\\" + tableName + ".json created!");
        }

        /// <summary>
        /// Get the SQL data table rows.
        /// </summary>
        /// <param name="tableName">It will be the data of this data table.</param>
        /// <returns></returns>
        private List<List<string>> GetTableData(string tableName) {
            Logger.Write("Retrieving data for " + tableName + "...");
            List<List<string>> rows = new List<List<string>>();
            foreach (var datas in this._SQL.Read("SELECT * FROM " + tableName)) {
                List<string> row = new List<string>();
                foreach (var data in datas) {
                    row.Add(data);
                }
                rows.Add(row);
            }
            Logger.Write(rows.Count + " data row(s) found!");
            return rows;
        }

        /// <summary>
        /// Get all the data tables names from SQL Server
        /// </summary>
        /// <returns></returns>
        private List<string> GetTablesName() {
            Logger.Write("Retrieving SQL tables name¸...");
            List<string> tablesName = new List<string>();
            foreach (var table in this._SQL.Read("SELECT name FROM sys.Tables")) {
                tablesName.Add(table[0]);
            }
            Logger.Write(tablesName.Count + " table(s) found!");
            return tablesName;
        }

        /// <summary>
        /// Get all columns names and data types from an SQL data table.
        /// </summary>
        /// <param name="tableName">SQL data table name.</param>
        /// <returns></returns>
        private List<string[]> GetTableColumnNames(string tableName) {
            Logger.Write("Retrieving columns name and data type for " + tableName + "...");
            List<string[]> columns = new List<string[]>();
            foreach (var column in this._SQL.Read("SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '" + tableName + "'")) {
                columns.Add(new string[] {
                    column[0],
                    column[1]
                });
            }
            Logger.Write(columns.Count + " column(s) found!");
            return columns;
        }
    }

    static class Logger {
        public static void Write(string content) {
            Console.WriteLine("[" + DateTime.Now.ToString("yyyy-MM-dd hh:mm:ss") + "]: " + content);
        }
    }

    /// <summary>
    /// Read the config.json file for SQL Server credentials
    /// </summary>
    public static class IO {
        public static Config Read() {
            return JsonConvert.DeserializeObject<Config>((File.ReadAllText(@".\config.json")));
        }
    }

    /// <summary>
    /// It's the OO version of the config.json file.
    /// </summary>
    public class Config {
        public string Server { get; set; }
        public string Database { get; set; }
        public string Username { get; set; }
        public string Password { get; set; }
        public int NbBackupBeforeArchive { get; set; }
    }

    /// <summary>
    /// It do read and write request to an SQL Server using plain SQL code.
    /// </summary>
    public class SQL {
        private SqlConnection Connection { get; set; }
        private SqlCommand Cmd { get; set; }

        public SQL(string server, string database) {
            Connection = new SqlConnection("Data Source=" + server + ";Initial Catalog=" + database + ";Integrated Security=SSPI;");
        }

        public SQL(string server, string database, string username, string password) {
            Connection = new SqlConnection("Data Source=" + server + ";Initial Catalog=" + database + ";UserId=" + username + ";Password=" + password + ";");
        }

        private bool InitializeConnection() {
            try {
                Connection.Open();
                Cmd = Connection.CreateCommand();
            } catch (Exception) {
                return false;
            }
            return true;
        }

        private bool CloseConnection() {
            try {
                Connection.Close();
                Cmd = null;
            } catch (Exception) {
                return false;
            }
            return true;
        }

        public List<List<string>> Read(string query) {
            List<List<string>> data = new List<List<string>>();
            if (InitializeConnection()) {
                string q = query.ToUpper();

                Cmd.CommandText = query;
                var reader = Cmd.ExecuteReader();

                int count = reader.FieldCount;
                while (reader.Read()) {
                    List<string> ls = new List<string>();
                    for (int i = 0; i < count; i++) {
                        ls.Add(reader[i].ToString());

                    }

                    if (ls.Count > 0) {
                        data.Add(ls);
                    }
                }

                CloseConnection();
            }
            return data;
        }

        public bool Write(string query) {
            if (InitializeConnection()) {
                string q = query.ToUpper();

                Cmd.CommandText = query;
                try {
                    Cmd.ExecuteNonQuery();
                } catch (Exception) {
                    Console.WriteLine("Connection crash");
                    CloseConnection();
                    return false;
                }
                CloseConnection();
                return true;
            } else {
                Console.WriteLine("Connection failed");
                CloseConnection();
                return false;
            }
        }
    }
}
