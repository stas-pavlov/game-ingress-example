using System;
using Microsoft.Azure.EventHubs;
using System.Threading.Tasks;

using System.IO;
using System.Text;
using System.Net;
using Newtonsoft.Json;

namespace GameDevice
{

    
    class Program
    {
        private static EventHubClient eventHubClient;
        private const string EhConnectionString = "Endpoint=sb://game-hack.servicebus.windows.net/;SharedAccessKeyName=Send;SharedAccessKey=tpIQ4lNYjbve068X+kpMvWAUUMQuy47CpA4i054OIfM=;EntityPath=data-ingress";
        private const string EhEntityPath = "data-ingress";

        static void Main(string[] args)
        {
            MainAsync(args).GetAwaiter().GetResult();
        }

        private static async Task MainAsync(string[] args)
        {
            // Creates an EventHubsConnectionStringBuilder object from the connection string, and sets the EntityPath.
            // Typically, the connection string should have the entity path in it, but for the sake of this simple scenario
            // we are using the connection string from the namespace.
            var connectionStringBuilder = new EventHubsConnectionStringBuilder(EhConnectionString)
            {
                EntityPath = EhEntityPath
            };

            eventHubClient = EventHubClient.CreateFromConnectionString(connectionStringBuilder.ToString());

            await SendMessagesToEventHub();

            await eventHubClient.CloseAsync();

            Console.WriteLine("Press ENTER to exit.");
            Console.ReadLine();
        }

        // Creates an event hub client and sends 100 messages to the event hub.
        private static async Task SendMessagesToEventHub()
        {
           var lines = File.ReadLines("data/visits.csv");


            foreach (string line in lines)
            {
                try
                {
                    
                    var messages = line.Split(',');
                    var eventData = new GameEventData();
                    eventData.event_name = messages[0];
                    eventData.event_time = messages[1];
                    eventData.platform = messages[2];
                    eventData.device_id = messages[3];
                    eventData.build = messages[4];
                    eventData.level = messages[5];
                    eventData.offline = messages[6];
                    eventData.country = messages[7];
                    eventData.device_type = messages[8];
                    eventData.install_time = messages[9];
                    eventData.playing_day = messages[10];

                    var message = JsonConvert.SerializeObject(eventData);

                    Console.WriteLine($"Sending message: {message}");
                    await eventHubClient.SendAsync(new EventData(Encoding.UTF8.GetBytes(message)));
                }
                catch (Exception exception)
                {
                    Console.WriteLine($"{DateTime.Now} > Exception: {exception.Message}");
                }

                await Task.Delay(10);
            }

            Console.WriteLine("All messages sent.");
        }

        class GameEventData
        {
            public string event_name;
            public string event_time;
            public string platform;
            public string device_id;
            public string build;
            public string level;
            public string offline;
            public string country;
            public string device_type;
            public string install_time;
            public string playing_day;
        }


    }
}