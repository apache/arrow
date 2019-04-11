package org.apache.arrow.memory;

import io.netty.buffer.ArrowBuf;

public interface OwnershipTransferResult {

  ArrowBuf getTransferredBuffer();
}
