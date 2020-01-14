using osm2mssql.Library.Protobuf;
using ProtoBuf;
using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net;
using System.Text;

namespace DLAddresses.Helper
{
    public class OpenStreetMapsHelper
    {
        public const int MaxDataBlockSize = 32 * 1024 * 1024;
        public const int MaxHeaderBlockSize = 64 * 1024;

        public string downloadData_fromOpenstreetmaps()
        {
            string NordrheinWestfalen = "http://download.geofabrik.de/europe/germany/nordrhein-westfalen-latest.osm.pbf";
            string Liechtenstein = "https://download.geofabrik.de/europe/liechtenstein-latest.osm.pbf"; // Zum Testen
            string RegierungsbezierkKoeln = "http://download.geofabrik.de/europe/germany/nordrhein-westfalen/koeln-regbez-latest.osm.pbf";

            var FileDownloadPath = Path.GetTempPath() + @"\Openstreetmap_Data.osm.pbf";

            if (File.Exists(FileDownloadPath))
            {
                File.Delete(FileDownloadPath);
            }

            using (var client = new WebClient())
            {
                client.DownloadFile(Liechtenstein, FileDownloadPath);
            }

            return FileDownloadPath;
        }

        public IEnumerable<Address> ReadAddresses(string fileName)
        {
            //var max_streetLength = 0;
            //var max_housenumberLength = 0;
            //var max_cityLength = 0;
            //var max_postcodeLength = 0;
            //var max_countryLength = 0;

            using (var file = File.Open(fileName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
            {
                BlobHeader blobHeader = null;

                while ((blobHeader = ReadBlobHeader(file)) != null)
                {
                    var block = ReadBlob(file, blobHeader) as PrimitiveBlock;
                    if (block != null)
                    {
                        foreach (PrimitiveGroup group in block.PrimitiveGroup)
                        {
                            if (group.Ways == null)
                                continue;
                            foreach (var way in group.Ways)
                            {
                                var returnAddress = new Address() { Country = "DE" };
                                bool anyAddress = false;

                                if (way.Keys != null)
                                {
                                    for (int i = 0; i < way.Keys.Count; i++)
                                    {
                                        string tagType = block.StringTable[way.Keys[i]];
                                        string tagValue = block.StringTable[way.Values[i]];

                                        //int tempCount = tagValue.Length;

                                        switch (tagType)
                                        {
                                            case "":
                                                break;

                                            case "addr:street":
                                                anyAddress = true;
                                                returnAddress.Street = tagValue;

                                                //max_streetLength = tempCount > max_streetLength ? tempCount : max_streetLength;
                                                break;
                                            case "addr:housenumber":
                                                anyAddress = true;
                                                returnAddress.Housenumber = tagValue;

                                                //max_housenumberLength = tempCount > max_housenumberLength ? tempCount : max_housenumberLength;
                                                break;
                                            case "addr:city":
                                                anyAddress = true;
                                                returnAddress.City = tagValue;

                                                //max_cityLength = tempCount > max_cityLength ? tempCount : max_cityLength;
                                                break;
                                            case "addr:postcode":
                                                anyAddress = true;
                                                returnAddress.Postcode = tagValue;

                                                //max_postcodeLength = tempCount > max_postcodeLength ? tempCount : max_postcodeLength;
                                                break;
                                            //case "addr:country":
                                            //    anyAddress = true;
                                            //    returnAddress.Country = tagValue;

                                            //    //max_countryLength = tempCount > max_countryLength ? tempCount : max_countryLength;
                                            //    break;
                                            default:
                                                break;
                                        }
                                    }
                                }

                                if (!anyAddress || string.IsNullOrEmpty(returnAddress.Street) || string.IsNullOrEmpty(returnAddress.Postcode) || string.IsNullOrEmpty(returnAddress.City))
                                {
                                    continue;
                                }


                                yield return returnAddress;
                            }
                        }
                    }
                }
            }
        }

        public DataTable convertAdressList_toDataTable(IEnumerable<Address> adresses)
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

            using (FastMember.ObjectReader reader = FastMember.ObjectReader.Create(adresses, "Id", "Street", "Housenumber", "City", "Postcode", "Country"))
            {
                dataTable.Load(reader);
            }

            return dataTable;
        }

        private BlobHeader ReadBlobHeader(Stream inputStream)
        {
            if (inputStream.Position < inputStream.Length)
            {
                return Serializer.DeserializeWithLengthPrefix<BlobHeader>(inputStream, PrefixStyle.Fixed32BigEndian);
            }
            return null;
        }

        private object ReadBlob(Stream inputStream, BlobHeader header)
        {
            var buffer = new byte[header.DataSize];
            inputStream.Read(buffer, 0, header.DataSize);
            Blob blob;
            using (var s = new MemoryStream(buffer))
            {
                blob = Serializer.Deserialize<Blob>(s);
            }

            Stream blobContentStream = null;
            try
            {
                if (blob.Raw != null)
                {
                    blobContentStream = new MemoryStream(blob.Raw);
                }
                else if (blob.ZlibData != null)
                {
                    var deflateStreamData = new MemoryStream(blob.ZlibData);
                    //skip ZLIB header
                    deflateStreamData.Seek(2, SeekOrigin.Begin);
                    blobContentStream = new DeflateStream(deflateStreamData, CompressionMode.Decompress);
                }

                if (header.Type.Equals("OSMData", StringComparison.InvariantCultureIgnoreCase))
                {
                    if ((blob.RawSize.HasValue && blob.RawSize > MaxDataBlockSize) ||
                        (blob.RawSize.HasValue == false && blobContentStream.Length > MaxDataBlockSize))
                    {
                        throw new InvalidDataException("Invalid OSMData block");
                    }

                    return Serializer.Deserialize<PrimitiveBlock>(blobContentStream);
                }
                else if (header.Type.Equals("OSMHeader", StringComparison.InvariantCultureIgnoreCase))
                {
                    if ((blob.RawSize.HasValue && blob.RawSize > MaxHeaderBlockSize) ||
                        (blob.RawSize.HasValue == false && blobContentStream.Length > MaxHeaderBlockSize))
                    {
                        throw new InvalidDataException("Invalid OSMHeader block");
                    }

                    return Serializer.Deserialize<OsmHeader>(blobContentStream);
                }
                else
                {
                    return null;
                }
            }
            finally
            {
                if (blobContentStream != null)
                {
                    blobContentStream.Close();
                    blobContentStream.Dispose();
                    blobContentStream = null;
                }
            }
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
    }
}