using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using System;
using System.Threading.Tasks;

namespace SendNotification
{
    class Program
    {
        private static IServiceProvider _serviceProvider;
        static async Task Main(string[] args)
        {
            RegisterServices();
            IServiceScope scope = _serviceProvider.CreateScope();
            await scope.ServiceProvider.GetRequiredService<Application>().Run();
            DisposeServices();
        }

        private static void RegisterServices()
        {
            IConfiguration configuration = new ConfigurationBuilder()
             .AddJsonFile("appsettings.json", true, true)
             .Build();

            var services = new ServiceCollection();
            services.AddSingleton(configuration);
            services.AddSingleton<Application>();
            _serviceProvider = services.BuildServiceProvider();
        }

        private static void DisposeServices()
        {
            if (_serviceProvider == null)
            {
                return;
            }
            if (_serviceProvider is IDisposable)
            {
                ((IDisposable)_serviceProvider).Dispose();
            }
        }
    }
}
