﻿using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using qimb.client.dotnet;

namespace Qimb.Client.DotNet
{
    public class QimbClient
    {
        private readonly string _endpoint;
        private readonly HttpClient _client = new HttpClient();
        private readonly TimeSpan _interval = TimeSpan.FromMilliseconds(300);
        private CancellationTokenSource _cancellationToken = new CancellationTokenSource();
        private readonly ConcurrentDictionary<string, bool> _receivedMessages = new ConcurrentDictionary<string, bool>();

        public Guid NodeId { get; set; }

        private QimbClient(string endpoint)
        {
            _endpoint = endpoint;
            NodeId = Guid.NewGuid();
        }

        public async Task<string> PublishMessageDirectAsync(string nodeId, string message)
        {
            var messageId = Guid.NewGuid().ToString();
            var url = this._endpoint + "message/publish/node/" + nodeId + "/" + messageId;

            await PublishMessageToUrl(message, url);

            return messageId;
        }

        public async Task<string> PublishMessageAsync(string messageType, string message)
        {
            var messageId = Guid.NewGuid().ToString();
            var url = this._endpoint + "message/publish/type/" + messageType + "/" + messageId;

            await PublishMessageToUrl(message, url);

            return messageId;
        }

        private async Task PublishMessageToUrl(string message, string url)
        {
            var request = new HttpRequestMessage()
            {
                RequestUri = new Uri(url),
                Method = HttpMethod.Put,
                Content = new ByteArrayContent(Encoding.UTF8.GetBytes(message)),
                Headers = {{"X-Qimb-NodeId", NodeId.ToString()}}
            };

            var response = await _client.SendAsync(request);

            if (response.StatusCode != HttpStatusCode.Created)
                throw new PublishException($"Unable to publish message");
        }

        public async Task SubscribeDirectAsync(string pushEndpoint = null)
        {
            var url = this._endpoint + "message/subscribe/node";

            await SubscribeFromUrl(url, pushEndpoint);
        }

        public async Task SubscribeAsync(string messageType, string pushEndpoint = null)
        {
            var url = this._endpoint + "message/subscribe/type/" + messageType;

            await SubscribeFromUrl(url, pushEndpoint);
        }

        private async Task SubscribeFromUrl(string url, string pushEndpoint)
        {
            var content = pushEndpoint != null
                ? "{\"pushEndpoint\":\"" + pushEndpoint + "\"}"
                : "{}";

            var request = new HttpRequestMessage()
            {
                RequestUri = new Uri(url),
                Method = HttpMethod.Put,
                Headers = {{"X-Qimb-NodeId", NodeId.ToString()}},
                Content = new ByteArrayContent(Encoding.UTF8.GetBytes(content)),
            };

            var response = await _client.SendAsync(request);

            if (response.StatusCode != HttpStatusCode.OK)
                throw new SubscribeException($"Unable to subscribe");
        }

        public async Task DeleteMessageAsync(string handle)
        {
            var url = this._endpoint + "message/delete/" + WebUtility.UrlEncode(handle);

            var request = new HttpRequestMessage()
            {
                RequestUri = new Uri(url),
                Method = HttpMethod.Delete,
                Headers = { { "X-Qimb-NodeId", NodeId.ToString() } }

            };

            var response = await _client.SendAsync(request);

            if (response.StatusCode != HttpStatusCode.OK)
                throw new DeleteException($"Unable to delete message");
        }

        public async Task ProcessSnsMessage(Stream bodyContent, Func<Envelope, Task> callback)
        {
            StreamReader reader = new StreamReader(bodyContent);
            var snsMessage = JsonConvert.DeserializeObject<SnsMessageDTO>(reader.ReadToEnd());

            if (snsMessage == null)
                return;

            if (snsMessage.Type == "SubscriptionConfirmation")
            {
                await this._client.GetAsync(snsMessage.SubscribeURL);
            }

            if (snsMessage.Type == "Notification")
            {
                var messageDto = JsonConvert.DeserializeObject<ReceiveMessageResponseDTO>(snsMessage.Message);

                if (IsMessageNew(messageDto.MessageId))
                {
                    await callback.Invoke(new Envelope(messageDto));
                }
            }
        }

        private bool IsMessageNew(string messageId)
        {
            return _receivedMessages.TryAdd(messageId, true);
        }

        public static QimbClient Setup(string endpoint)
        {
            if (!endpoint.EndsWith("/"))
                endpoint += "/";

            return new QimbClient(endpoint);
        }


        public void BeginRecieve(Func<Envelope, Task> callback)
        {
            RunAsync(() => ReceiveAsync(callback));
        }

        private async Task ReceiveAsync(Func<Envelope, Task> callback)
        {
            var url = this._endpoint + "message/receive";

            var request = new HttpRequestMessage()
            {
                RequestUri = new Uri(url),
                Method = HttpMethod.Get,
                Headers = { { "X-Qimb-NodeId", NodeId.ToString() } }
            };
            try
            {
                var response = await _client.SendAsync(request);

                if (response.StatusCode != HttpStatusCode.OK)
                    throw new ReceiveException($"Unable to receive messages");

                var messageStream = await response.Content.ReadAsStreamAsync();
                var reader = new StreamReader(messageStream);

                var messageDTOs = JsonConvert.DeserializeObject<ReceiveMessageResponseDTO[]>(reader.ReadToEnd());

                List<Task> tasks = new List<Task>();
                foreach (var messageDto in messageDTOs)
                {
                    tasks.Add(ExecuteMessage(callback, messageDto));
                }

                await Task.WhenAll(tasks);
            }
            catch (Exception ex)
            {
                throw new ReceiveException($"Unable to receive messages", ex);
            }
        }

        private async Task ExecuteMessage(Func<Envelope, Task> callback, ReceiveMessageResponseDTO messageDto)
        {
            if (IsMessageNew(messageDto.MessageId))
                await callback.Invoke(new Envelope(messageDto));
            await DeleteMessageAsync(messageDto.ReceiptHandle);
        }

        private async void RunAsync(Func<Task> callback)
        {
            while (true)
            {
                Task delayTask = Task.Delay(this._interval, this._cancellationToken.Token);

                try
                {
                    await delayTask.ConfigureAwait(false);
                    await callback.Invoke().ConfigureAwait(false);
                }
                catch (TaskCanceledException)
                {
                    return;
                }
            }
        }
    }

    public class ReceiveMessageResponseDTO
    {
        [JsonProperty("message")]
        public string Message { get; set; }
        [JsonProperty("messageId")]
        public string MessageId { get; set; }
        [JsonProperty("messageType")]
        public string MessageType { get; set; }
        [JsonProperty("receiptHandle")]
        public string ReceiptHandle { get; set; }
        [JsonProperty("senderNodeId")]
        public string SenderNodeId { get; set; }
    }

    public class SnsMessageDTO
    {
        [JsonProperty]
        public string Type { get; set; }
        [JsonProperty]
        public string Message { get; set; }
        [JsonProperty]
        public string SubscribeURL { get; set; }
    }
}
