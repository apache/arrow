using System.Net;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.Hosting;

namespace Apache.Arrow.Flight.Sql.TestWeb;

public class Program
{
    public static void Main(string[] args)
    {
        CreateHostBuilder(args).Build().Run();
    }

    private static IHostBuilder CreateHostBuilder(string[] args) =>
        Host.CreateDefaultBuilder(args)
            .ConfigureWebHostDefaults(webBuilder =>
            {
                webBuilder
                    .ConfigureKestrel((context, options) =>
                    {
                        if (context.HostingEnvironment.IsDevelopment())
                        {
                            options.Listen(IPEndPoint.Parse("0.0.0.0:5001"), l => l.Protocols = HttpProtocols.Http2);
                        }
                    })
                    .UseStartup<Startup>();
            });
}
