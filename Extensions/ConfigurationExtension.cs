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
}

public record DbInfo(string Type, string ConnectionString);

