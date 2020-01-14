using DLAddresses.Helper;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Net;

namespace OSM2Database_Minimal_Reproduction
{
    class Program
    {
        static void Main(string[] args)
        {
            OpenStreetMapsHelper OsmHelper = new OpenStreetMapsHelper();

            string FileDownloadPath = OsmHelper.downloadData_fromOpenstreetmaps();

            IEnumerable<Address> addresses = OsmHelper.ReadAddresses(FileDownloadPath);

            DataTable dataTable = OsmHelper.convertAdressList_toDataTable(addresses);

            using (SqlConnection connection = new SqlConnection("I am a Connectionstring"))
            {
                connection.Open();
                SqlTransaction transaction = connection.BeginTransaction();

                try
                {
                    SqlCommand command = connection.CreateCommand();

                    command.Connection = connection;
                    command.Transaction = transaction;
                    command.CommandText = "TRUNCATE TABLE Addresses";
                    command.ExecuteNonQuery();

                    using (var sql_bulk_copy = new SqlBulkCopy(connection, SqlBulkCopyOptions.Default, transaction))
                    {
                        //sql_bulk_copy.NotifyAfter = 10000;
                        //sql_bulk_copy.SqlRowsCopied += (sender, eventArgs) => Helper.OpenStreetMapsHelper.logToFile("Wrote " + eventArgs.RowsCopied + " records...");
                        sql_bulk_copy.BatchSize = 10000;
                        sql_bulk_copy.DestinationTableName = "Addresses";
                        sql_bulk_copy.WriteToServer(dataTable);
                    }

                    transaction.Commit();
                }
                catch (Exception ex1)
                {
                    try
                    {
                        transaction.Rollback();
                    }
                    catch (Exception ex2)
                    {
                    }
                }
            }
        }
    }
}
