using System;
using System.Collections.Generic;
using System.Text;
using Google.Protobuf;

namespace Apache.Arrow.Flight
{
    public class Ticket
    {
        private readonly Protocol.Ticket _ticket;
        internal Ticket(Protocol.Ticket ticket)
        {
            _ticket = ticket;
        }

        public Ticket(ByteString ticket)
        {
            _ticket = new Protocol.Ticket()
            {
                Ticket_ = ticket
            };
        }

        public Ticket(string ticket)
            : this(ByteString.CopyFromUtf8(ticket))
        {
        }

        public Ticket(byte[] bytes)
            : this(ByteString.CopyFrom(bytes))
        {
        }

        public string TicketString => _ticket.Ticket_.ToStringUtf8();

        public ByteString TicketByteString => _ticket.Ticket_;

        public byte[] TicketBytes => _ticket.Ticket_.ToByteArray();

        public Protocol.Ticket ToProtocol()
        {
            return _ticket;
        }
    }
}
