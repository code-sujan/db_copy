﻿using System.Data;
using System.Data.SqlClient;
using Application.Extensions;
using Application.Helpers;
using Dapper;
using Microsoft.Extensions.Configuration;
using Spectre.Console;

namespace Application.Services;

public interface ISqlToSqlService
{
    List<string> Copy(StatusContext ctx, SqlConnection sourceConnection, SqlConnection destinationConnection);
}

public class SqlToSqlService : ISqlToSqlService
{
    private readonly IConfiguration _configuration;

    public SqlToSqlService(IConfiguration configuration)
    {
        _configuration = configuration;
    }

    public List<string> Copy(StatusContext ctx, SqlConnection sourceConnection, SqlConnection destinationConnection)
    {
        var errors = new List<string>();
        sourceConnection.Open();
        destinationConnection.Open();

        var getSchemasQuery = "SELECT schema_name FROM information_schema.schemata";
        var allSchemas = sourceConnection.Query<string>(getSchemasQuery).ToList();
        SpectreConsoleHelper.Log("Fetched schemas from source...");

        var schemas = GetNecessarySchemas(allSchemas);

        ctx.Status("Looping through available schemas...");
        var copyOptions = _configuration.GetCopyOptions();
        foreach (var sourceSchema in schemas)
        {
            string destinationSchema = $"{sourceSchema}";

            // CreateSchema(ctx, destinationConnection, destinationSchema);

            ctx.Status($"Fetching available tables from {sourceSchema} schema...");
            var getTablesQuery = $"SELECT table_name FROM information_schema.tables WHERE table_schema = '{sourceSchema}'";
            var tables = sourceConnection.Query<string>(getTablesQuery).ToList();
            SpectreConsoleHelper.Log($"Fetched tables of {sourceSchema} schema from postgres");

            ctx.Status($"Looping through all tables of {sourceSchema} schema...");
            foreach (var table in tables)
            {
                // CreateTable(ctx, sourceConnection, destinationConnection, table, sourceSchema, destinationSchema);

                IDataReader data;
                try
                {
                    ctx.Status($"Fetching data from {sourceSchema}.{table}");
                    data = sourceConnection.ExecuteReader($"SELECT * FROM {sourceSchema}.{table}");
                    SpectreConsoleHelper.Log($"Fetched data from {sourceSchema}.{table} table");

                    var dataTable = new DataTable();
                    dataTable.Load(data);

                    ctx.Status($"Transferring data of [blue]{sourceSchema}.{table}[/]");
                    using var bulkCopy = new SqlBulkCopy(destinationConnection);
                    bulkCopy.DestinationTableName = $"{destinationSchema}.{table}";
                    bulkCopy.BulkCopyTimeout = 300;
                    bulkCopy.WriteToServer(dataTable);
                    SpectreConsoleHelper.Success($"Successfully transferred data of {sourceSchema}.{table}");
                }
                catch (Exception ex)
                {
                    errors.Add($"{sourceSchema}~{table}");
                    AnsiConsole.WriteException(ex);
                }
            }
        }
        return errors;
    }

    private static void CreateSchema(StatusContext ctx, SqlConnection destinationConnection, string destinationSchema)
    {
        ctx.Status($"Creating {destinationSchema} schema in sql server...");
        var createDestinationSchemaQuery = $"CREATE SCHEMA [{destinationSchema}];";
        destinationConnection.Execute(createDestinationSchemaQuery);
        SpectreConsoleHelper.Log($"Created {destinationSchema} schema in sql server...");
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
            "db_denydatawriter",
            "HangFire"
        };

        return schemas.Where(x => !defaultSchemas.Contains(x)).ToList();
    }

    private static void CreateTable(StatusContext ctx, SqlConnection sourceConnection, SqlConnection destinationConnection, string table, string sourceSchema, string destinationSchema)
    {
        ctx.Status($"Fetching column definition for {table} table...");
        var getColumnsQuery = $"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{table}' AND table_schema = '{sourceSchema}'";
        var columns = sourceConnection.Query(getColumnsQuery);
        SpectreConsoleHelper.Log($"Fetched column definition for {table} table...");

        ctx.Status($"Creating table {destinationSchema}.{table} in sql server...");
        var createTableQuery = $"CREATE TABLE {destinationSchema}.{table} (";
        createTableQuery += string.Join(", ", columns.Select(column => $"[{column.column_name}] {column.data_type}"));
        createTableQuery += ")";
        destinationConnection.Execute(createTableQuery);
        SpectreConsoleHelper.Log($"Created table {destinationSchema}.{table} in sql server...");
    }
}