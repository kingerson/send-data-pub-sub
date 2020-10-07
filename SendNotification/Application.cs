using Dasync.Collections;
using Google.Apis.Auth.OAuth2;
using Google.Cloud.PubSub.V1;
using Google.Protobuf;
using Grpc.Auth;
using Microsoft.Extensions.Configuration;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net.Http;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace SendNotification
{
    public class Application
    {
        private IConfiguration _configuration;
        private PublisherServiceApiClient _client;

        public Application(
            IConfiguration configuration
            )
        {
            _configuration = configuration;
        }

        public async Task Run()
        {
            var urlFile = @"https://saeu2c001archnhuat01.blob.core.windows.net/hub-notification/dataNotification.gz";

            var file = await DownloadFile(urlFile);
            var fileDescompress = await Decompress(file);

            var filePath = Path.GetTempFileName();

            using (FileStream fileJson = File.Create(filePath))
                await fileJson.WriteAsync(fileDescompress, 0, fileDescompress.Length);

            var data = await File.ReadAllTextAsync(filePath, Encoding.UTF8);
            var modelo = JsonConvert.DeserializeObject<NotificationProcess>(data);

            var notifications = GetListNotificationPush(modelo);

            await ProcessNotification(notifications);

        }

        public async Task RunLocalData()
        {
            var notifications = GenerateListNotificationPush();

            await ProcessNotification(notifications);
        }

        #region Methods
        public async Task<byte[]> DownloadFile(string url)
        {
            using (var client = new HttpClient())
            {

                using (var result = await client.GetAsync(url))
                {
                    if (result.IsSuccessStatusCode)
                    {
                        return await result.Content.ReadAsByteArrayAsync();
                    }

                }
            }
            return null;
        }
        public async Task<byte[]> Decompress(byte[] file)
        {
            using var to = new MemoryStream();
            using var from = new MemoryStream(file);
            using var compress = new GZipStream(from, CompressionMode.Decompress);
            await compress.CopyToAsync(to);
            return to.ToArray();
        }
        private async Task ProcessNotification(IEnumerable<NotificationPush> notifications)
        {
            var sent_date = GetDateToBigQuery();
            var notification_messages = new List<NotificationMessage>();

            foreach (var m in notifications)
            {
                var notification_id = GenerateNotificationId();
                var notification_message = new NotificationMessage
                {
                    rp_notification_id = notification_id,
                    rp_sequential_id = m.id_sequencial,
                    register_date = sent_date,
                    message = m.notification.body,
                    uuid = m.uuid,
                    state = 1
                };
                notification_messages.Add(notification_message);
            }

            var total = notification_messages.Count();

            Console.WriteLine($"Cantidad de notificaciones Enviadas: {notifications.Count()}");

            Console.WriteLine($"Total notification_messages a enviar al Pub/Sub: {total}");


            if (total == notifications.Count())
            {
                var maximum_message = Convert.ToInt32(_configuration["MaximumMessage"]);

                var iteration = total < maximum_message ? 1 : total / maximum_message;

                Console.WriteLine($"Total de iteracciones: {iteration}");

                var listNotifications = new List<IEnumerable<NotificationMessage>>();

                var skip = 0;

                for (int i = 0; i < iteration; i++)
                {
                    var filter = notification_messages.Skip(skip).Take(maximum_message);

                    filter.Select(m =>
                    {
                        var rp_notification_id = m.rp_notification_id;
                        m.rp_notification_id = $"GRUPO-{i}";
                        return m;
                    }).ToList();

                    listNotifications.Add(filter);
                    skip += maximum_message;
                }

                Console.WriteLine($"Inicia el envio al pub sub {GetDate()}");

                await listNotifications.ParallelForEachAsync(async m =>
                {
                    try
                    {
                        await PublishToTopicBash(m);
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"Error");
                        Console.WriteLine($"Model :{JsonConvert.SerializeObject(m)}");
                        Console.WriteLine($"Exception :{JsonConvert.SerializeObject(ex)}");
                        //throw;
                    }

                }, maxDegreeOfParallelism: 0);

                Console.WriteLine($"Finaliza el envio al pub sub {GetDate()}");
            }
            else
            {
                Console.WriteLine($"La cantidad de notificaciones a procesar por PUB SUB no es la misma a las notificaciones enviadas");
                Console.WriteLine($"Cantidad a Enviada: {notifications.Count()}");
                Console.WriteLine($"Cantidad a Procesar: {total}");
            }
        }

        private IEnumerable<NotificationPush> GetListNotificationPush(NotificationProcess notifications)
        {
            var result = new List<NotificationPush>();
            foreach (var item in notifications.user_devices)
            {
                var noti = new NotificationPush
                {
                    to = item.device_token,
                    uuid = item.uuid,
                    id_sequencial = notifications.id_sequencial,
                    application_code = notifications.application?.id.ToString(),
                    campaign_id = notifications.campaign?.id.ToString(),
                    token = notifications.notification.server_key,
                    data = notifications.notification.data,
                    notification = new NotificationModel
                    {
                        title = notifications.notification.notification.title,
                        body = notifications.notification.notification.body,
                    }
                };

                result.Add(noti);
            }

            return result.ToArray();
        }


        private IEnumerable<NotificationPush> GenerateListNotificationPush()
        {
            var result = new List<NotificationPush>();
            for (int i = 1; i < 200001; i++)
            {
                var item = new NotificationPush()
                {
                    to = $"cwD6fF4RRH2XhZvy1HMtKt:APA91bFoLm8ZX5t4CsTbWk5xNXvJ62Lp9VX3Jagde4qJh_bdxcRmSBHQdiOvilG5-30_SKTf6s1m-4k-WJrB04l_3YLehYPfpYKjj542Qrdv4hDQUSimjE2Nf91khuwhmnSNSyszVCD-{i}",
                    uuid = GenerateNotificationId(),
                    id_sequencial = "060",
                    application_code = "001",
                    campaign_id = "001",
                    token = "AAAArdy8rx8:APA91bFIc7SAxEUNWQZacbv3xyQ36_kRFyIGmf99Feftv-PKCarXfp4MTb2yrRqrdLhAizwSfP8Dg1ToZVbO30Rzz4Ji9Dpa7T8vGoXETbZpg-1Kz6Vs5s3Ozo7PBGg0IH3bRVxlPOXk",
                    data = new { rp_sequential_id = "060", rp_notification_id = "060", other_parameter = "Developer" },
                    notification = new NotificationModel
                    {
                        title = "Test PubSub",
                        body = "Mensaje Pub Sub para probar la distribucion de las notificaciones de manera masiva en un azure function"
                    }
                };
                result.Add(item);
            }

            return result.ToArray();
        }

        private string GenerateUUID()
        {
            long ticks = DateTime.Now.Ticks;
            byte[] bytes = BitConverter.GetBytes(ticks);
            string id = Convert.ToBase64String(bytes)
                                    .Replace('+', '_')
                                    .Replace('/', '-')
                                    .TrimEnd('=');
            return id;
        }

        private string GenerateNotificationId()
        {
            return $"{GetDate().ToString("yyyyMMddhhmmssfff")}";
        }
       
        public DateTime GetDateToBigQuery()
        {
            TimeZoneInfo cstZone = TimeZoneInfo.FindSystemTimeZoneById(_configuration["TimeZone"]);
            DateTime cstTime = TimeZoneInfo.ConvertTime(DateTime.Now, cstZone);
            return cstTime.AddTicks(-(cstTime.Ticks % TimeSpan.TicksPerSecond));
        }
        public DateTime GetDate()
        {
            TimeZoneInfo cstZone = TimeZoneInfo.FindSystemTimeZoneById(_configuration["TimeZone"]);
            DateTime cstTime = TimeZoneInfo.ConvertTime(DateTime.Now, cstZone);
            return cstTime;
        }
        #endregion

        #region Method PubSub
        public async Task PublishToTopicBash<T>(IEnumerable<T> model)
        {
            var topicName = new TopicName(_configuration["GCP_ProjectId"], _configuration["GCP_TopicId"]);
            var _publisherClient = await CreateClient();

            var messages = new List<PubsubMessage>();

            foreach (var item in model)
            {
                var json = JsonConvert.SerializeObject(item, Formatting.None);

                var message = new PubsubMessage()
                {
                    Data = ByteString.CopyFromUtf8(json)
                };

                messages.Add(message);
            }

            var response = await _publisherClient.PublishAsync(topicName, messages);
        }

        public async Task<PublisherServiceApiClient> CreateClient()
        {
            var binDirectory = Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location);

            var rootDirectory = Path.GetFullPath(Path.Combine(binDirectory, "../../.."));

            var config = Path.Combine(rootDirectory, "authgcp.json");

            GoogleCredential credential = null;
            using (var jsonStream = new FileStream(config, FileMode.Open, FileAccess.Read, FileShare.Read))
                credential = GoogleCredential.FromStream(jsonStream);

            PublisherServiceApiClient publisherService = await new PublisherServiceApiClientBuilder { ChannelCredentials = credential.ToChannelCredentials() }.BuildAsync();

            _client = publisherService;

            return _client;
        }
        #endregion

        #region Models
        public class NotificationPush
        {
            public string to { get; set; }
            public string uuid { get; set; }
            public string id_sequencial { get; set; }
            public string application_code { get; set; }
            public string campaign_id { get; set; }
            public List<string> registration_ids { get; set; }
            public string token { get; set; }
            public dynamic data { get; set; }
            public NotificationModel notification { get; set; }
            public bool content_available { get; set; } = true;
        }
        public class NotificationProcess
        {
            public string id_sequencial { get; set; }
            public ApplicationModel application { get; set; }
            public CampaignModel campaign { get; set; }
            public NotificationPushModel notification { get; set; }
            public IEnumerable<UserDeviceModel> user_devices { get; set; }
        }
        public class NotificationMessage
        {
            public string rp_notification_id { get; set; }
            public string rp_sequential_id { get; set; }
            public string message { get; set; }
            public string uuid { get; set; }
            public DateTime register_date { get; set; }
            public int state { get; set; }
            public string type { get; set; } = "NC";
        }
        public class NotificationPushModel
        {
            public dynamic data { get; set; }
            public string server_key { get; set; }
            public NotificationModel notification { get; set; }
        }
        public class ApplicationModel
        {
            public int id { get; set; }
            public string name { get; set; }
        }
        public class CampaignModel
        {
            public int id { get; set; }
            public string name { get; set; }
        }
        public class UserDeviceModel
        {
            public string uuid { get; set; }
            public string identifier { get; set; }
            public string device_token { get; set; }
            public string name { get; set; }
            public string last_name { get; set; }
            public string document_number { get; set; }
        }
        public class NotificationModel
        {
            public string body { get; set; }
            public string title { get; set; }
            public string click_action { get; set; }
            public string icon { get; set; }
        }
        #endregion

    }
}
