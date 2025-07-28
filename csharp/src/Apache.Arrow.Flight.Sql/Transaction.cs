// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

namespace Apache.Arrow.Flight.Sql;

using System;
using Google.Protobuf;

public readonly struct Transaction : IEquatable<Transaction>
{
    private static readonly ByteString TransactionIdDefaultValue = ByteString.Empty;

    private readonly ByteString _transactionId;

    public ByteString TransactionId => _transactionId ?? TransactionIdDefaultValue;

    public static readonly Transaction NoTransaction = new(TransactionIdDefaultValue);

    public Transaction(ByteString transactionId)
    {
        _transactionId = ProtoPreconditions.CheckNotNull(transactionId, nameof(transactionId));
    }

    public Transaction(string transactionId)
    {
        _transactionId = ByteString.CopyFromUtf8(transactionId);
    }

    public bool IsValid => TransactionId.Length > 0;

    public override bool Equals(object? obj)
    {
        if (obj is not Transaction other)
            return false;

        return Equals(other);
    }

    public bool Equals(Transaction other)
    {
        // Safe compare even if _transactionId is null (from default(Transaction))  
        return (_transactionId ?? TransactionIdDefaultValue)
            .Equals(other._transactionId);
    }

    public override int GetHashCode() => (_transactionId ?? TransactionIdDefaultValue).GetHashCode();

    public static bool operator ==(Transaction left, Transaction right) => left.Equals(right);
    public static bool operator !=(Transaction left, Transaction right) => !left.Equals(right);
}
