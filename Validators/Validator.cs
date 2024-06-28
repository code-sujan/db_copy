using System.Data;
using Application.Helpers;
using Application.Validators.Interfaces;

namespace Application.Validators;

internal class Validator : IValidator
{
    public void ValidateProviders(IDbConnection source, IDbConnection dest)
    {
        if (!IsServerConnected(dest))
        {
            SpectreConsoleHelper.Error($"Invalid db connection {source.ConnectionString}");
            Console.ReadLine();
        }
        SpectreConsoleHelper.Success("Db connected...");
        
        if (!IsServerConnected(source))
        {
            SpectreConsoleHelper.Error($"Invalid db connection {source.ConnectionString}");
            Console.ReadLine();
        }
        SpectreConsoleHelper.Success("Db connected...");
    }

    #region Private methods

    private static bool IsServerConnected(IDbConnection connection)
    {
        try
        {
            connection.Open();
            return true;
        }
        catch (Exception)
        {
            return false;
        }
    }

    #endregion
}
