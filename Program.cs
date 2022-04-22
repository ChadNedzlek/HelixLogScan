// See https://aka.ms/new-console-template for more information

using System.Data;
using System.Diagnostics;
using System.Text.RegularExpressions;
using Kusto.Data;
using Kusto.Data.Common;
using Kusto.Data.Net.Client;
using Kusto.Data.Results;

namespace HelixLogScan
{
    public class KustoSchema
    {
        private List<(string name, string type)> _columns;

        private KustoSchema(List<(string name, string type)> columns)
        {
            _columns = columns;
        }

        public int FieldCount => _columns.Count;

        public int GetOrdinal(string name)
        {
            return _columns.FindIndex(c => string.Equals(c.name, name, StringComparison.OrdinalIgnoreCase));
        }

        public string GetName(int ordinal)
        {
            return _columns[ordinal].name;
        }

        public string GetKustoType(int ordinal)
        {
            return _columns[ordinal].type;
        }

        public string GetKustoType(string name) => GetKustoType(GetOrdinal(name));

        public Type GetFieldType(int ordinal)
        {
            string kustoType = GetKustoType(ordinal);
            return kustoType switch
            {
                "long" => typeof(long),
                "int" => typeof(int),
                "datetime" => typeof(DateTime),
                "string" => typeof(string),
                _ => throw new InvalidOperationException($"No type mapping for type '{kustoType}'")
            };
        }
        
        public Type GetFieldType(string name) => GetFieldType(GetOrdinal(name));

        public static KustoSchema FromSchemaTable(DataTable table)
        {
            return new KustoSchema(table.Rows.Cast<DataRow>().Select(r => ((string)r[0], (string)r[1])).ToList());
        }
    }

    public static class Program
    {
        public static async Task Main(string[] args)
        {
            using var client = new HttpClient();
            int scanned = 0;
            Stopwatch s = Stopwatch.StartNew();
            SemaphoreSlim sem = new SemaphoreSlim(50, 50);
            await foreach (var uri in EnumerateConsoleUris())
            {
                await sem.WaitAsync();
                Task.Run(
                    async () =>
                    {
                        try
                        {
                            var val = Interlocked.Increment(ref scanned);
                            if (val % 100 == 0)
                            {
                                Console.ForegroundColor = ConsoleColor.Gray;
                                Console.WriteLine($"Scanned {val} in {s.Elapsed} ({s.Elapsed / val} per item)");
                                Console.ResetColor();
                            }

                            if (!Uri.IsWellFormedUriString(uri, UriKind.Absolute))
                            {
                                return;
                            }

                            using HttpResponseMessage response = await client.GetAsync(
                                uri,
                                HttpCompletionOption.ResponseHeadersRead
                            );

                            if (response.Content.Headers.ContentLength <= 100_000_000)
                            {
                                using var reader = new StreamReader(await response.Content.ReadAsStreamAsync());
                                string line;
                                while ((line = await reader.ReadLineAsync()) != null)
                                {
                                    if (Regex.IsMatch(line, "No space left on device", RegexOptions.IgnoreCase))
                                    {
                                        Console.WriteLine(line);
                                        break;
                                    }
                                }
                            }
                        }
                        finally
                        {
                            sem.Release();
                        }
                    }
                );
            }
        }

        private static async IAsyncEnumerable<string> EnumerateConsoleUris()
        {
            var kcsb = new KustoConnectionStringBuilder("https://engsrvprod.kusto.windows.net/", "engineeringdata")
                .WithAadUserPromptAuthentication();
            using var client = KustoClientFactory.CreateCslQueryProvider(kcsb);
            var opts = new ClientRequestProperties();
            opts.SetOption(ClientRequestProperties.OptionResultsProgressiveEnabled, true);
            opts.SetOption(ClientRequestProperties.OptionProgressiveProgressReportPeriod, "10s");
            var queryResult = await client.ExecuteQueryV2Async(
                "engineeringdata",
                "WorkItems | where ExitCode != 0 | order by Finished desc | project Uri=ConsoleUri",
                opts
            );
            var e = queryResult.GetFrames();
            KustoSchema currentSchema = null;
            while (e.MoveNext())
            {
                switch (e.Current)
                {
                    case ProgressiveDataSetDataTableSchemaFrame schema:
                    {
                        // Console.WriteLine($"Found schema {schema.TableName}: {string.Join(",", schema.TableSchema.Rows.Cast<DataRow>().Select(r => $"{r[0]}:{r[1]}"))}");
                        currentSchema = KustoSchema.FromSchemaTable(schema.TableSchema);
                        break;
                    }
                    case ProgressiveDataSetDataTableFragmentFrame fragment:
                    {
                        int index = currentSchema.GetOrdinal("Uri");
                        if (index == -1 || currentSchema.GetKustoType(index) != "string")
                        {
                            break;
                        }

                        object[] values = new object[fragment.FieldCount];
                        while (fragment.GetNextRecord(values))
                        {
                            object uri = values[index];
                            yield return (string)uri;
                        }

                        break;
                    }
                    case ProgressiveDataSetDataTableFrame dataTable:
                    {
                        var reader = dataTable.TableData;
                        switch (dataTable.TableKind)
                        {
                            default:
                            {
                                // Console.WriteLine($"Skipping table {dataTable.TableName}");
                                do
                                {
                                    while (reader.Read())
                                    {
                                    }
                                } while (reader.NextResult());

                                reader.Dispose();
                                // dataTable.TableData.WriteAsText("PIZZA", true, s);
                                // dataTable.TableData.Close();
                                break;
                            }
                        }

                        break;
                    }
                    default:
                    {
                        // Console.WriteLine($"Skipping frame type: {e.Current.FrameType}");
                        break;
                    }
                }
            }
        }
    }
}