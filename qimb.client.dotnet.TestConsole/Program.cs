using System;
using System.Threading.Tasks;

namespace Qimb.Client.DotNet.TestConsole
{
    class Program
    {
        private static QimbClient _client = QimbClient
            .Setup("https://glb92op7l0.execute-api.eu-west-1.amazonaws.com/v2/");

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
            _client.SubscribeAsync(messageType).Wait();
            Console.WriteLine($"Subscribed for {messageType} with node id {_client.NodeId}");
            _client.BeginRecieve((envelope) =>
            {
                Console.WriteLine($"Message of type '{envelope.MessageType}' with id '{envelope.MessageId}'");
                Console.WriteLine($"Received from '{envelope.SenderNodeId}'");
                Console.WriteLine(envelope.Message);
                return Task.FromResult(true);
            });
        }

        static void Publish(string[] args)
        {
            var messageType = args[1];
            var message = args[2];
            var messageId = _client.PublishMessageAsync(messageType, message).Result;
            Console.WriteLine($"Sent message with id '{messageId}' from '{_client.NodeId}'");
        }
    }
}
