using Microsoft.Extensions.Configuration;

namespace Application.Extensions;

public static class ConfigurationExtension
{
    public static DbInfo GetSource(this IConfiguration configuration)
    {
        return new DbInfo(configuration["Source:Type"], configuration["Source:ConnectionString"]);
    }
    
    public static DbInfo GetDestination(this IConfiguration configuration)
    {
        return new DbInfo(configuration["Destination:Type"], configuration["Destination:ConnectionString"]);
    }

    public static CopyOptions GetCopyOptions(this IConfiguration configuration)
    {
        var opt = configuration.GetSection("Options").Get<CopyOptions>() ?? new CopyOptions();
        if (!opt.CopySelected) return new CopyOptions();
        return opt;
    }
}

public class CopyOptions
{
    public bool CopySelected { get; set; }
    public List<CopyInfo> Infos { get; set; }
}

public class CopyInfo
{
    public string Schema { get; set; }
    public List<string> Tables { get; set; }
}

public record DbInfo(string Type, string ConnectionString);

