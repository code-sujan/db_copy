using System.ComponentModel;
using System.Data;
using System.Data.SqlClient;
using System.Text;
using Application.Extensions;
using Application.Helpers;
using Application.Services.Interfaces;
using Application.Validators.Interfaces;
using Dapper;
using Microsoft.Extensions.Configuration;
using Npgsql;
using NpgsqlTypes;
using Spectre.Console;

namespace Application.Services;

internal class Service : IService
{
    private readonly IValidator _validator;
    private readonly IConfiguration _configuration;
    private readonly ISqlToSqlService _sqlToSqlService;

    public Service(
        IValidator validator, IConfiguration configuration, ISqlToSqlService sqlToSqlService)
    {
        _validator = validator;
        _configuration = configuration;
        _sqlToSqlService = sqlToSqlService;
    }

    public void Migrate()
    {
        var source = _configuration.GetSource();
        var destination = _configuration.GetDestination();
        if (source.Type.ToLower().Equals("mssql") && destination.Type.ToLower().Equals("mssql"))
        {
            MigrateSqlToSql(source.ConnectionString, destination.ConnectionString);
        }
        else if (source.Type.ToLower().Equals("mssql") && destination.Type.ToLower().Equals("psql"))
        {
            SqlToPsql(source.ConnectionString, destination.ConnectionString);
        }
        else if (source.Type.ToLower().Equals("psql") && destination.Type.ToLower().Equals("mssql"))
        {
            PsqlToSql(source.ConnectionString, destination.ConnectionString);
        }
    }

    private void MigrateSqlToSql(string source, string dest)
    {
        var errors = new List<string>();
        try
        {
            SpectreConsoleHelper.WriteHeader("MSSQL To MSSQL", Color.Blue);
            using var sourceConnection = new SqlConnection(source);
            using var destConnection = new SqlConnection(dest);
            _validator.ValidateProviders(sourceConnection, destConnection);
            SpectreConsoleHelper.Log("Initializing...");
            AnsiConsole.Status()
                .Spinner(Spinner.Known.Arrow3)
                .SpinnerStyle(Style.Parse("green"))
                .Start("Starting the migration...", ctx =>
                {
                    errors = _sqlToSqlService.Copy(ctx, sourceConnection, destConnection);
                });
            SpectreConsoleHelper.WriteHeader("Success!", Color.Green);
        }
        catch (Exception ex)
        {
            AnsiConsole.WriteException(ex);
        }
        finally
        {
            DoOnFinally(errors);
        }
    }

    void PsqlToSql(string source, string dest)
    {
        var errors = new List<string>();
        try
        {
            SpectreConsoleHelper.WriteHeader("postgresql to mssql", Color.Blue);
            SpectreConsoleHelper.Log("Initializing...");
            AnsiConsole.Status()
                .Spinner(Spinner.Known.Arrow3)
                .SpinnerStyle(Style.Parse("green"))
                .Start("Starting the migration...", ctx =>
                {
                    using var postgresConnection = new NpgsqlConnection(source);
                    using var sqlServerConnection = new SqlConnection(dest);
                    _validator.ValidateProviders(postgresConnection, sqlServerConnection);

                    postgresConnection.Open();
                    sqlServerConnection.Open();

                    ctx.Status("Fetching postgresql schemas");
                    ctx.Spinner(Spinner.Known.BouncingBall);
                    var getSchemasQuery = "SELECT schema_name FROM information_schema.schemata";
                    var schemas = postgresConnection.Query<string>(getSchemasQuery).ToList();
                    SpectreConsoleHelper.Log("Fetched schemas from postgresql...");

                    RemoveUnnecessarySchemas(schemas);

                    ctx.Status("Looping through available schemas...");
                    foreach (var sourceSchema in schemas)
                    {
                        string destinationSchema = $"{sourceSchema}_new";

                        ctx.Status($"Creating {destinationSchema} schema in sql server...");
                        var createDestinationSchemaQuery = $"CREATE SCHEMA [{destinationSchema}];";
                        sqlServerConnection.Execute(createDestinationSchemaQuery);
                        SpectreConsoleHelper.Log($"Created {destinationSchema} schema in sql server...");

                        ctx.Status($"Fetching available tables from {sourceSchema} schema...");
                        var getTablesQuery = $"SELECT table_name FROM information_schema.tables WHERE table_schema = '{sourceSchema}'";
                        var tables = postgresConnection.Query<string>(getTablesQuery).ToList();
                        SpectreConsoleHelper.Log($"Fetched tables of {sourceSchema} schema from postgres");

                        ctx.Status($"Looping through all tables of {sourceSchema} schema...");
                        foreach (var table in tables)
                        {
                            ctx.Status($"Fetching column definition for {table} table...");
                            var getColumnsQuery = $"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{table}' AND table_schema = '{sourceSchema}'";
                            var columns = postgresConnection.Query(getColumnsQuery);
                            SpectreConsoleHelper.Log($"Fetched column definition for {table} table...");

                            ctx.Status($"Creating table {destinationSchema}.{table} in sql server...");
                            var createTableQuery = $"CREATE TABLE {destinationSchema}.{table} (";
                            createTableQuery += string.Join(", ", columns.Select(column => $"[{column.column_name}] {ConvertPostgreSqlToSqlServerDataType(column.data_type)}"));
                            createTableQuery += ")";
                            sqlServerConnection.Execute(createTableQuery);
                            SpectreConsoleHelper.Log($"Created table {destinationSchema}.{table} in sql server...");

                            IDataReader data;
                            try
                            {
                                ctx.Status($"Fetching data from {sourceSchema}.{table} from postgresql...");
                                data = postgresConnection.ExecuteReader($"SELECT * FROM {sourceSchema}.{table}");
                                SpectreConsoleHelper.Log($"Fetched data from {sourceSchema}.{table} table of postgresql...");

                                ctx.Status("Coverting the data into proper shape before migrating to sql server...");
                                var dataTable = new DataTable();
                                dataTable.Load(data);
                                SpectreConsoleHelper.Log("Converted data into proper shape...");

                                ctx.Status($"Transferring data from [blue]{sourceSchema}.{table}[/] to [green]{destinationSchema}.{table}[/]");
                                using var bulkCopy = new SqlBulkCopy(sqlServerConnection);
                                bulkCopy.DestinationTableName = $"{destinationSchema}.{table}";
                                bulkCopy.BulkCopyTimeout = 300;
                                bulkCopy.WriteToServer(dataTable);
                                SpectreConsoleHelper.Success($"Successfully transferred data from {sourceSchema}.{table} to {destinationSchema}.{table}");
                            }
                            catch (Exception ex)
                            {
                                errors.Add($"{sourceSchema}~{table}");
                                AnsiConsole.WriteException(ex);
                            }
                        }
                    }
                });
            SpectreConsoleHelper.WriteHeader("Success!", Color.Green);
        }
        catch (Exception ex)
        {
            AnsiConsole.WriteException(ex);
        }
        finally
        {
            DoOnFinally(errors);
        }
    }

    void SqlToPsql(string source, string dest)
    {
        var errors = new List<string>();

        try
        {
            SpectreConsoleHelper.WriteHeader("MSSql to psql", Color.Blue);

            SpectreConsoleHelper.Log("Initializing...");
            AnsiConsole.Status()
                .Spinner(Spinner.Known.Arrow3)
                .SpinnerStyle(Style.Parse("green"))
                .Start("Starting the migration...", ctx =>
                {
                    using var sourceConnection = new SqlConnection(source);
                    using var destConnection = new NpgsqlConnection(dest);
                    _validator.ValidateProviders(sourceConnection, destConnection);

                    sourceConnection.Open();
                    destConnection.Open();

                    ctx.Status("Fetching MSSQL schemas");
                    ctx.Spinner(Spinner.Known.BouncingBall);
                    var getSchemasQuery = "SELECT schema_name FROM information_schema.schemata";
                    var allSchemas = sourceConnection.Query<string>(getSchemasQuery).ToList();
                    SpectreConsoleHelper.Log("Fetched schemas from postgresql...");

                    var schemas = GetNecessarySchemas(allSchemas);

                    ctx.Status("Looping through available schemas...");
                    foreach (var sourceSchema in schemas)
                    {
                        string destinationSchema = $"{sourceSchema}_new";

                        ctx.Status($"Creating {destinationSchema} schema in sql server...");
                        var createDestinationSchemaQuery = $"CREATE SCHEMA {destinationSchema};";
                        destConnection.Execute(createDestinationSchemaQuery);
                        SpectreConsoleHelper.Log($"Created {destinationSchema} schema in sql server...");

                        ctx.Status($"Fetching available tables from {sourceSchema} schema...");
                        var getTablesQuery = $"SELECT table_name FROM information_schema.tables WHERE table_schema = '{sourceSchema}'";
                        var tables = sourceConnection.Query<string>(getTablesQuery).ToList();
                        SpectreConsoleHelper.Log($"Fetched tables of {sourceSchema} schema from postgres");

                        ctx.Status($"Looping through all tables of {sourceSchema} schema...");
                        foreach (var table in tables)
                        {
                            ctx.Status($"Fetching column definition for {table} table...");
                            var getColumnsQuery = $"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{table}' AND table_schema = '{sourceSchema}'";
                            var columns = sourceConnection.Query(getColumnsQuery);
                            SpectreConsoleHelper.Log($"Fetched column definition for {table} table...");

                            ctx.Status($"Creating table {destinationSchema}.{table} in sql server...");
                            var createTableQuery = $"CREATE TABLE {destinationSchema}.{table} (";
                            createTableQuery += string.Join(", ", columns.Select(column => $"{CleanColumn(column.column_name)} {ConvertSqlTypeToPsqlType(column.data_type)}"));
                            createTableQuery += ")";
                            destConnection.Execute(createTableQuery);
                            SpectreConsoleHelper.Log($"Created table {destinationSchema}.{table} in sql server...");

                            IDataReader data;
                            try
                            {
                                ctx.Status($"Fetching data from {sourceSchema}.{table} from postgresql...");
                                data = sourceConnection.ExecuteReader($"SELECT * FROM {sourceSchema}.{table}");
                                SpectreConsoleHelper.Log($"Fetched data from {sourceSchema}.{table} table of postgresql...");

                                ctx.Status("Coverting the data into proper shape before migrating to sql server...");
                                var dataTable = new DataTable();
                                dataTable.Load(data);
                                SpectreConsoleHelper.Log($"Converted data into proper shape... Data Count: {dataTable.Rows.Count}");

                                ctx.Status($"Transferring data from [blue]{sourceSchema}.{table}[/] to [green]{destinationSchema}.{table}[/]");
                                PsqlBulkInsert(destConnection, dataTable, $"{destinationSchema}.{table}", createTableQuery);
                                SpectreConsoleHelper.Success($"Successfully transferred data from {sourceSchema}.{table} to {destinationSchema}.{table}");
                            }
                            catch (Exception ex)
                            {
                                errors.Add($"{sourceSchema}~{table}");
                                AnsiConsole.WriteException(ex);
                            }
                        }
                    }
                });
            SpectreConsoleHelper.WriteHeader("Success!", Color.Green);
        }
        catch (Exception ex)
        {
            AnsiConsole.WriteException(ex);
        }
        finally
        {
            DoOnFinally(errors);
        }
    }

    private string CleanColumn(string columnName)
    {
        var col =  columnName.Replace("%", "").Replace(' ', '_');
        if (col.Contains(' ')) return CleanColumn(col);
        return col;
    }

    private static void DoOnFinally(List<string> errors)
    {
        if (errors.Any())
        {
            var table = new Table();
            table.Title("List of failed migration table/views");

            table.AddColumn("SourceSchema");
            table.AddColumn("SourceTable");

            foreach (var error in errors)
            {
                var errorDetails = error.Split("~");
                table.AddRow(errorDetails[0], errorDetails[1]);
            }

            table.Border(TableBorder.Rounded);
            AnsiConsole.Write(table);
        }
    }


    #region Private methods

    private static void RemoveUnnecessarySchemas(List<string> schemas)
    {
        if (schemas.Contains("information_schema"))
        {
            schemas.Remove("information_schema");
        }

        if (schemas.Contains("pg_catalog"))
        {
            schemas.Remove("pg_catalog");
        }

        if (schemas.Contains("pg_toast"))
        {
            schemas.Remove("pg_toast");
        }

        if (schemas.Contains("pg_temp_1"))
        {
            schemas.Remove("pg_temp_1");
        }

        if (schemas.Contains("pg_toast_temp_1"))
        {
            schemas.Remove("pg_toast_temp_1");
        }
    }

    private static string ConvertPostgreSqlToSqlServerDataType(string postgresDataType)
    {
        var map = new Dictionary<string, string>
        {
            { "bigint", "bigint" },
            { "boolean", "bit" },
            { "character", "char" },
            { "character varying", "nvarchar(max)" },
            { "date", "date" },
            { "double precision", "float" },
            { "integer", "int" },
            { "interval", "time" },
            { "numeric", "decimal" },
            { "real", "real" },
            { "smallint", "smallint" },
            { "text", "nvarchar(max)" },
            { "time", "time" },
            { "timestamp", "datetime2" },
            { "timestamptz", "datetimeoffset" },
            { "uuid", "uniqueidentifier" },
            { "bytea", "varbinary(max)" },
            { "bit", "bit" },
            { "bit varying", "varbinary(max)" },
            { "money", "money" },
            { "json", "nvarchar(max)" },
            { "jsonb", "nvarchar(max)" },
            { "cidr", "nvarchar(max)" },
            { "inet", "nvarchar(max)" },
            { "macaddr", "nvarchar(max)" },
            { "tsvector", "nvarchar(max)" },
            { "tsquery", "nvarchar(max)" },
            { "array", "nvarchar(max)" },
            { "domain", "nvarchar(max)" },
            { "timestamp with time zone", "datetimeoffset" },
        };

        return map.TryGetValue(postgresDataType.ToLower(), out string? value) ? value.ToUpper() : "nvarchar(max)".ToUpper();
    }

    private static List<string> GetNecessarySchemas(List<string> schemas)
    {
        var defaultSchemas = new List<string>
        {
            "guest",
            "INFORMATION_SCHEMA",
            "sys",
            "db_owner",
            "db_accessadmin",
            "db_securityadmin",
            "db_ddladmin",
            "db_backupoperator",
            "db_datareader",
            "db_datawriter",
            "db_denydatareader",
            "db_denydatawriter"
        };

        return schemas.Where(x => !defaultSchemas.Contains(x)).ToList();
    }

    public static string ConvertSqlTypeToPsqlType(string sqlType)
    {
        switch (sqlType.ToLower())
        {
            case "bigint":
                return "bigint";
            case "binary":
            case "varbinary":
            case "image":
                return "bytea";
            case "bit":
                return "boolean";
            case "char":
            case "nchar":
                return "char";
            case "date":
            case "datetime":
            case "datetime2":
            case "smalldatetime":
                return "timestamp";
            case "decimal":
            case "numeric":
            case "money":
            case "smallmoney":
            case "float":
            case "real":
                return "numeric";
            case "int":
                return "integer";
            case "nvarchar":
            case "varchar":
            case "text":
            case "ntext":
                return "text";
            case "smallint":
                return "smallint";
            case "time":
                return "time";
            case "timestamp":
                return "timestamp";
            case "uniqueidentifier":
                return "uuid";
            default:
                return sqlType;
        }
    }

    public void PsqlBulkInsert(NpgsqlConnection connection, DataTable dataTable, string tableName, string tableQuery)
    {
        var cmdTxt = $"COPY {tableName} ({string.Join(",", dataTable.Columns.Cast<DataColumn>().Select(c => CleanColumn(c.ColumnName)))}) FROM STDIN (FORMAT BINARY)";
        var types = new List<string>();
        var count = 0;
        using (var importer = connection.BeginBinaryImport(cmdTxt))
        {
            // Add each row to the binary importer
            foreach (DataRow row in dataTable.Rows)
            {
                importer.StartRow();
                for (int i = 0; i < dataTable.Columns.Count; i++)
                {
                    var value = row[i];
                    var type = dataTable.Columns[i].DataType;
                    if (count == 0)
                    {
                        types.Add(type.FullName);
                    }
                    NpgsqlDbType dataType = GetNpgsqlDbType(type);
                    if (value == null || value == DBNull.Value)
                    {
                        importer.WriteNull();
                    }
                    else
                    {
                        // var byteVal = ConvertToBytes(value, type);
                        importer.Write(value, dataType);
                    }
                }

                count++;
            }

           
            try
            {
                importer.Complete();
            }
            catch (Exception e)
            {
                Console.WriteLine($"{tableQuery}");
                Console.WriteLine($"{string.Join(",",types)}");
                connection.Open();
            }
        }
    }
    
    static NpgsqlDbType GetNpgsqlDbType(Type type)
    {
        
        
        if (type == typeof(byte[]))
        {
            return NpgsqlDbType.Bytea;
        }
        else if (type == typeof(bool))
        {
            return NpgsqlDbType.Boolean;
        }
        else if (type == typeof(char))
        {
            return NpgsqlDbType.Char;
        }
        else if (type == typeof(DateTime))
        {
            return NpgsqlDbType.Timestamp;
        }
        else if (type == typeof(decimal))
        {
            return NpgsqlDbType.Numeric;
        }
        else if (type == typeof(double))
        {
            return NpgsqlDbType.Numeric;
        }
        else if (type == typeof(float))
        {
            return NpgsqlDbType.Numeric;
        }
        else if (type == typeof(Guid))
        {
            return NpgsqlDbType.Uuid;
        }
        else if (type == typeof(int))
        {
            return NpgsqlDbType.Integer;
        }
        else if (type == typeof(long))
        {
            return NpgsqlDbType.Bigint;
        }
        else if (type == typeof(short))
        {
            return NpgsqlDbType.Smallint;
        }
        else if (type == typeof(TimeSpan))
        {
            return NpgsqlDbType.Time;
        }
        else if (type == typeof(DateTimeOffset))
        {
            return NpgsqlDbType.TimestampTz;
        }
        else if (type == typeof(string))
        {
            return NpgsqlDbType.Text;
        }
        else
        {
            throw new Exception($"Invalid type {type.FullName}");
        }
    }
    
    #endregion
}