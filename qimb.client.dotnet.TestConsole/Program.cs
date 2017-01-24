using System;
using System.Diagnostics;
using System.Net;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using qimb.client.dotnet;

namespace Qimb.Client.DotNet.TestConsole
{
    class Program
    {
        private static QimbClient _client = QimbClient
            .Setup("url to api gateway");

        static void Main(string[] args)
        {
            var action = args.Length > 0 ? args[0] : null;
            switch (action)
            {
                case "publish":
                    Publish(args);
                    return;
                case "subscribe":
                    Receive(args);
                    Console.ReadLine();
                    return;
                default:
                    Console.WriteLine("publish <messagetype> <message>");
                    Console.WriteLine("subscribe <messagetype>");
                    return;
            }
        }

        private static void Receive(string[] args)
        {
            var messageType = args[1];
            var pushUrl = args.Length > 2 ? args[2] : null;

            StartListner(pushUrl, null);

            _client.SubscribeAsync(messageType, pushUrl).Wait();

            //Console.WriteLine($"Subscribed for {messageType} with node id {_client.NodeId}");
            _client.BeginRecieve(ReceiveCallback);
        }

        private static Task ReceiveCallback(Envelope envelope)
        {
            Console.WriteLine(envelope.Message);

            if (envelope.Message.StartsWith("reply"))
            {
                PublishDirect(envelope.SenderNodeId, envelope.Message.Substring(6));
            }

            return Task.FromResult(true);
        }

        private static async void StartListner(string pushUrl, Stopwatch stopwatch)
        {
            if (pushUrl == null)
                return;
            HttpListener listener = new HttpListener();
            listener.Prefixes.Add(pushUrl);
            listener.Start();

            while (true)
            {
                HttpListenerContext context = await listener.GetContextAsync();
                HttpListenerRequest request = context.Request;


                if (stopwatch != null && stopwatch.IsRunning)
                {
                    stopwatch.Stop();
                    Console.WriteLine($"Response time in ms: {stopwatch.ElapsedMilliseconds}");
                }

                await _client.ProcessSnsMessage(request.InputStream, ReceiveCallback);
                        
                HttpListenerResponse response = context.Response;
                response.StatusCode = 200;
                response.Close();
            }
        }

        static void Publish(string[] args)
        {
            var messageType = args[1];
            var message = args[2];
            var pushUrl = args.Length > 3 ? args[3] : null;

            Stopwatch stopwatch = new Stopwatch();
            if (message.StartsWith("reply"))
            {
                StartListner(pushUrl, stopwatch);
                ReceiveDirect(pushUrl, stopwatch);
            }

            Thread.Sleep(1000);

            while (true)
            {
                stopwatch.Reset();
                stopwatch.Start();

                Publish(messageType, message);
                Console.ReadLine();
            }
        }

        private static void ReceiveDirect(string publishEndpoint, Stopwatch stopwatch)
        {
            _client.SubscribeDirectAsync(publishEndpoint).Wait();
            _client.BeginRecieve((envelope) =>
            {
                stopwatch?.Stop();

                //Console.WriteLine($"Message of type '{envelope.MessageType}' with id '{envelope.MessageId}'");
                //Console.WriteLine($"Received from '{envelope.SenderNodeId}'");
                //Console.WriteLine(envelope.Message);
                if (stopwatch != null)
                    Console.WriteLine($"Response time in ms: {stopwatch.ElapsedMilliseconds}");

                return Task.FromResult(true);
            });
        }

        static void Publish(string messageType, string message)
        {
            var messageId = _client.PublishMessageAsync(messageType, message).Result;
            //Console.WriteLine($"Sent message with id '{messageId}' from '{_client.NodeId}'");
        }

        static void PublishDirect(string nodeId, string message)
        {
            var messageId = _client.PublishMessageDirectAsync(nodeId, message).Result;
            //Console.WriteLine($"Sent message with id '{messageId}' from '{_client.NodeId}'");
        }

    }
}
