using OsmSharp;
using OsmSharp.Streams;
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
            string FileDownloadPath = DownloadOSMData();
            DataTable dataTable = getDataTable_fromOSMFile(FileDownloadPath);
            importDataTable_toDatabase(dataTable);

            Console.ReadKey();
        }

        public static string DownloadOSMData()
        {
            Console.WriteLine("Trying to get Data from OpenStreetMaps...");

            string NordrheinWestfalen = "http://download.geofabrik.de/europe/germany/nordrhein-westfalen-latest.osm.pbf";
            string Liechtenstein = "https://download.geofabrik.de/europe/liechtenstein-latest.osm.pbf"; // testing
            string RegierungsbezierkKoeln = "http://download.geofabrik.de/europe/germany/nordrhein-westfalen/koeln-regbez-latest.osm.pbf";

            var FileDownloadPath = Path.GetTempPath() + @"\Openstreetmap_Data.osm.pbf";

            if (File.Exists(FileDownloadPath))
            {
                File.Delete(FileDownloadPath);
            }

            using (var client = new WebClient())
            {
                client.DownloadFile(RegierungsbezierkKoeln, FileDownloadPath);
            }

            return FileDownloadPath;
        }

        public static DataTable getDataTable_fromOSMFile(string FileDownloadPath)
        {

            Console.WriteLine("Finished Downloading. Reading File into Stream...");

            using (var fileStream = new FileInfo(FileDownloadPath).OpenRead())
            {
                PBFOsmStreamSource source = new PBFOsmStreamSource(fileStream);

                if (source.Any() == false)
                {
                    return new DataTable();
                }

                Console.WriteLine("Finished Reading File into Stream. Filtering and Formatting RawData to Addresses...");
                Console.WriteLine();

                DataTable dataTable = convertAdressList_toDataTable(
                    source.Where(x => x.Type == OsmGeoType.Way && x.Tags.Count > 0 && x.Tags.ContainsKey("addr:street"))
                            .Select(Address.fromOSMGeo)
                            .Distinct(new AddressComparer())
                );

                return dataTable;
            }
        }

        private static DataTable convertAdressList_toDataTable(IEnumerable<Address> adresses)
        {
            DataTable dataTable = new DataTable();

            if (adresses.Any() == false)
            {
                return dataTable;
            }

            dataTable.Columns.Add("Id");
            dataTable.Columns.Add("Street");
            dataTable.Columns.Add("Housenumber");
            dataTable.Columns.Add("City");
            dataTable.Columns.Add("Postcode");
            dataTable.Columns.Add("Country");

            Int32 counter = 0;

            Console.WriteLine("Finished Filtering and Formatting. Writing Addresses From Stream to a DataTable Class for the Database-SQLBulkCopy-Process ");

            foreach (Address address in adresses)
            {
                dataTable.Rows.Add(counter + 1, address.Street, address.Housenumber, address.City, address.Postcode, address.Country);
                counter++;

                if (counter % 10000 == 0 && counter != 0)
                {
                    Console.WriteLine("Wrote " + counter + " Rows From Stream to DataTable.");
                }
            }

            return dataTable;
        }

        public static void importDataTable_toDatabase(DataTable dataTable)
        {
            string connectionString = "Look at me. Im a Connectionstring";

            using (SqlConnection connection = new SqlConnection(connectionString))
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

                    using (SqlBulkCopy sqlBulk = new SqlBulkCopy(connection, SqlBulkCopyOptions.KeepIdentity, transaction))
                    {
                        sqlBulk.NotifyAfter = 10000;
                        sqlBulk.SqlRowsCopied += (sender, eventArgs) => Console.WriteLine("Wrote " + eventArgs.RowsCopied + " records...");
                        sqlBulk.DestinationTableName = "Addresses";
                        sqlBulk.WriteToServer(dataTable);
                    }

                    transaction.Commit();
                }
                catch
                {
                    Console.WriteLine("Something went wront. Trying Rollback now...");

                    try
                    {
                        transaction.Rollback();

                        Console.WriteLine("Rollback completed...");
                        return;
                    }
                    catch (Exception ex2)
                    {
                        Console.WriteLine("Error during Rollback. Rollback could not be completed...");
                        return;
                    }
                }
            }

            Console.WriteLine("Import successful");

            return;
        }
    }

    public class Address
    {
        public int Id { get; set; }

        public string Street { get; set; }
        public string Housenumber { get; set; }
        public string City { get; set; }
        public string Postcode { get; set; }
        public string Country { get; set; }

        public static Func<OsmGeo, Address> fromOSMGeo = x =>
        {
            var result = new Address();

            if (x != null && x.Tags != null && x.Tags.Count > 0)
            {
                x.Tags.TryGetValue("addr:street", out string street);
                x.Tags.TryGetValue("addr:housenumber", out string housenumber);
                x.Tags.TryGetValue("addr:city", out string city);
                x.Tags.TryGetValue("addr:postcode", out string postcode);
                x.Tags.TryGetValue("addr:country", out string country);

                result.Street = street;
                result.Housenumber = housenumber;
                result.City = city;
                result.Postcode = postcode;
                result.Country = country;
            };

            return result;
        };
    }

    public class AddressComparer : IEqualityComparer<Address>
    {
        // Wird benötigt für Except Methodenaufrufe die Objekte nicht nur nach der ID vergleichen sollen
        // ... sondern auch nach allen anderen Eigenschaften

        public bool Equals(Address x, Address y)
        {
            if (x.Street != y.Street)
            {
                return false;
            }

            if (x.Housenumber != y.Housenumber)
            {
                return false;
            }

            if (x.City != y.City)
            {
                return false;
            }

            if (x.Postcode != y.Postcode)
            {
                return false;
            }

            if (x.Country != y.Country)
            {
                return false;
            }

            return true;
        }

        public int GetHashCode(Address address)
        {
            return address.Id.GetHashCode();
        }
    }
}
