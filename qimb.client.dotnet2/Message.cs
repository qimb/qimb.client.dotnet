using Qimb.Client.DotNet;

namespace qimb.client.dotnet
{
    public class Envelope
    {

        public Envelope(ReceiveMessageResponseDTO receiveMessageDto)
        {
            Message = receiveMessageDto.Message;
            MessageId = receiveMessageDto.MessageId;
            MessageType = receiveMessageDto.MessageType;
            SenderNodeId = receiveMessageDto.SenderNodeId;
        }

        public string Message { get; private set; }
        public string MessageId { get; private set; }
        public string MessageType { get; private set; }
        public string SenderNodeId { get; private set; }
    }
}
