using Application;
using Application.Services.Interfaces;
using Application.Validators;
using Application.Validators.Interfaces;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

var services = new ServiceCollection();

ConfigureServices(services);
var serviceProvider = services.BuildServiceProvider();
var service = serviceProvider.GetService<IService>();
service?.Migrate();
Console.ReadLine();

void ConfigureServices(ServiceCollection serviceCollection)
{
    serviceCollection.AddSingleton<IConfiguration>(BuildAndGetConfiguration());
    serviceCollection.AddTransient<IValidator, Validator>();
    serviceCollection.AddTransient<IService, Service>();
    serviceCollection.AddTransient<ISqlToSqlService, SqlToSqlService>();

    static IConfigurationRoot BuildAndGetConfiguration()
    {
        var config = new ConfigurationBuilder()
            .SetBasePath(AppContext.BaseDirectory)
            .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
            .Build();
        return config;
    }
}