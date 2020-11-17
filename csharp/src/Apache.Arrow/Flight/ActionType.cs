using System;
using System.Collections.Generic;
using System.Text;

namespace Apache.Arrow.Flight
{
    public class ActionType
    {
        private readonly Protocol.ActionType _actionType;
        public ActionType(Protocol.ActionType actionType)
        {
            _actionType = actionType;
        }

        public ActionType(string type, string description)
        {
            _actionType = new Protocol.ActionType()
            {
                Description = description,
                Type = type
            };
        }

        public string Type => _actionType.Type;
        public string Description => _actionType.Description;

        public Protocol.ActionType ToProtocol()
        {
            return _actionType;
        }

        public override bool Equals(object obj)
        {
            if(obj is ActionType other)
            {
                return Equals(_actionType, other._actionType);
            }
            return false;
        }

        public override int GetHashCode()
        {
            return _actionType.GetHashCode();
        }
    }
}
