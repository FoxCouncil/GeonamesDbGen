using Microsoft.Data.Sqlite;
using System.IO.Compression;

var httpClient = new HttpClient();

var cityCount = 15000;

const string inCountryUrl = "http://download.geonames.org/export/dump/countryInfo.txt";
const string inAdmin1Url = "http://download.geonames.org/export/dump/admin1CodesASCII.txt";
string inCitiesUrl = $"http://download.geonames.org/export/dump/cities{cityCount}.zip";

string inCountry = "countryInfo.txt";
string inAdmin1 = "admin1CodesASCII.txt";
string inCities = $"cities{cityCount}.txt";
string inCitiesZip = $"cities{cityCount}.zip";

await Task.WhenAll(DownloadFile(inCountryUrl, inCountry), DownloadFile(inAdmin1Url, inAdmin1), DownloadFile(inCitiesUrl, inCitiesZip));

if (!File.Exists(inCities))
{
    ZipFile.ExtractToDirectory(inCitiesZip, Directory.GetCurrentDirectory());
}

using var connection = new SqliteConnection($"Data Source=geonames.db");

connection.Open();

using var command = connection.CreateCommand() ?? throw new NullReferenceException();

command.CommandText = "DROP TABLE IF EXISTS geoname";
command.ExecuteNonQuery();

command.CommandText = "CREATE TABLE geoname (geonameid int PRIMARY KEY, name nvarchar(200), asciiname nvarchar(200), alternatenames nvarchar(4000), latitude decimal(18,15), longitude decimal(18,15), fclass nchar(1), fcode nvarchar(10), country nvarchar(2), cc2 nvarchar(60), admin1 nvarchar(20), admin2 nvarchar(80), admin3 nvarchar(20), admin4 nvarchar(20), population int, elevation int, gtopo30 int, timezone nvarchar(40), moddate date)";
command.ExecuteNonQuery();

command.CommandText = "DROP TABLE IF EXISTS admin1";
command.ExecuteNonQuery();

command.CommandText = "CREATE TABLE admin1 (key TEXT PRIMARY KEY, name nvarchar(200) NOT NULL, asciiname nvarchar(200) NOT NULL, geonameid int NOT NULL)";
command.ExecuteNonQuery();

command.CommandText = "DROP TABLE IF EXISTS country";
command.ExecuteNonQuery();

command.CommandText = "CREATE TABLE country (ISO TEXT PRIMARY KEY, ISO3 TEXT NOT NULL, IsoNumeric TEXT NOT NULL, fips TEXT NOT NULL, Country TEXT NOT NULL, Capital TEXT NOT NULL, Area INT NOT NULL, Population INT NOT NULL, Continent TEXT NOT NULL, tld TEXT NOT NULL, CurrencyCode TEXT NOT NULL, CurrencyName TEXT NOT NULL, Phone TEXT NOT NULL, PostalCodeFormat TEXT, PostalCodeRegex TEXT, Languages TEXT NOT NULL, geonameid INT NOT NULL, neighbours TEXT NOT NULL, EquivalentFipsCode TEXT NOT NULL)";
command.ExecuteNonQuery();

using var transaction = connection.BeginTransaction();
using var fileCommand = connection.CreateCommand();

DoFile(fileCommand, inCountry, "country", 19);
DoFile(fileCommand, inCities, "geoname", 19);
DoFile(fileCommand, inAdmin1, "admin1", 4);

transaction.Commit();

command.CommandText = "ALTER TABLE admin1 ADD iso3166_2 TEXT;";
command.ExecuteNonQuery();

// Let's do ISO 3166-2

command.CommandText = "SELECT * FROM admin1";
var alterReader = command.ExecuteReader();

using var isoTransaction = connection.BeginTransaction();
using var isoCommand = connection.CreateCommand();

DoIso3116_2Names(alterReader, isoCommand);

isoTransaction.Commit();

command.CommandText = "DROP TABLE IF EXISTS geoname_fulltext";
command.ExecuteNonQuery();

command.CommandText = "CREATE VIRTUAL TABLE geoname_fulltext USING fts3(geonameid int, longname text, asciiname text, admin1 text, country text, population int, latitude real, longitude real, timezone text)";
command.ExecuteNonQuery();

command.CommandText = "INSERT INTO geoname_fulltext SELECT g.geonameid, g.asciiname||', '||a.iso3166_2||' '||a.asciiname||', '||c.Country, g.asciiname, a.asciiname, c.Country, g.population, g.latitude, g.longitude, g.timezone FROM geoname g, admin1 a, country c WHERE g.country = c.ISO AND g.country||'.'||g.admin1 = a.key";
command.ExecuteNonQuery();

connection.Close();

async Task DownloadFile(string url, string fileName)
{
    if (File.Exists(fileName))
    {
        Console.WriteLine($"Skipping {url} file {fileName}");

        return;
    }

    Console.WriteLine($"Downloading {url} file {fileName}");
    using var stream = await httpClient.GetStreamAsync(url);
    using var fileStream = new FileStream(fileName, FileMode.CreateNew);
    await stream.CopyToAsync(fileStream);
    Console.WriteLine($"  COMPLETED {url} file {fileName}");
}

void DoFile(SqliteCommand command, string infile, string tableName, int expectedFields)
{
    if (command == null)
    {
        throw new NullReferenceException();
    }

    string sql = "INSERT INTO " + tableName + " VALUES (@f0";

    for (int x = 0; x < expectedFields - 1; x++)
    {
        sql += $",@f{x + 1}";
    }

    sql += ")";

    command.CommandText = sql;

    using var reader = new StreamReader(infile, System.Text.Encoding.UTF8) ?? throw new NullReferenceException();

    int i = 0;

    while (!reader.EndOfStream)
    {
        string line = reader.ReadLine() ?? throw new NullReferenceException();

        if (line.StartsWith("#"))
        {
            continue;
        }

        string[] fields = line.Split('\t');

        if (fields.Length != expectedFields)
        {
            Console.WriteLine(infile + ":" + i + ": got " + fields.Length + " fields (expected " + expectedFields + ")");

            continue;
        }

        for (int j = 0; j < expectedFields; j++)
        {
            command.Parameters.AddWithValue($"@f{j}", fields[j]);
        }

        command.ExecuteNonQuery();
        command.Parameters.Clear();

        if (i++ % 1000 == 0)
        {
            Console.WriteLine($"Processed {tableName} records {i}");
        }
    }

    reader.Close();

}

void DoIso3116_2Names(SqliteDataReader command, SqliteCommand isoCommand)
{
    var admin1IsoCodes = GetAdmin1IsoCodes();

    while (command.Read())
    {
        var key = alterReader.GetString(0);
        var codes = key.Split(".");

        if (admin1IsoCodes.ContainsKey(codes[0]))
        {
            var countriesIsoCodes = admin1IsoCodes[codes[0]];

            if (countriesIsoCodes.ContainsKey(codes[1]))
            {
                isoCommand.CommandText = "UPDATE admin1 SET iso3166_2 = @f0 WHERE key = @f1";
                isoCommand.Parameters.AddWithValue("@f0", countriesIsoCodes[codes[1]]);
                isoCommand.Parameters.AddWithValue("@f1", key);
                isoCommand.ExecuteNonQuery();

                isoCommand.Parameters.Clear();
            }
        }
        else if (codes[0] == "US")
        {
            isoCommand.CommandText = "UPDATE admin1 SET iso3166_2 = @f0 WHERE key = @f1";
            isoCommand.Parameters.AddWithValue("@f0", codes[1]);
            isoCommand.Parameters.AddWithValue("@f1", key);
            isoCommand.ExecuteNonQuery();

            isoCommand.Parameters.Clear();
        }
        else
        {
            isoCommand.CommandText = "UPDATE admin1 SET iso3166_2 = @f0 WHERE key = @f1";
            isoCommand.Parameters.AddWithValue("@f0", "");
            isoCommand.Parameters.AddWithValue("@f1", key);
            isoCommand.ExecuteNonQuery();

            isoCommand.Parameters.Clear();
        }
    }

    command.Close();
}

Dictionary<string, Dictionary<string, string>> GetAdmin1IsoCodes()
{
    return new Dictionary<string, Dictionary<string, string>>()
    {
        {
            "CA", new () {
                { "01", "AB" },
                { "02", "BC" },
                { "03", "MB" },
                { "04", "NB" },
                { "05", "NL" },
                { "07", "NS" },
                { "08", "ON" },
                { "09", "PE" },
                { "10", "QC" },
                { "11", "SK" },
                { "12", "YT" },
                { "13", "NT" },
                { "14", "NU" },
            }
        },
        {
            "AU", new () {
                { "01", "ACT" },
                { "02", "NSW" },
                { "03", "NT" },
                { "04", "QLD" },
                { "05", "SA" },
                { "06", "TAS" },
                { "07", "VIC" },
                { "08", "WA" },
            }
        }
    };
}