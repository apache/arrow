module arrow.memory.netty {
    exports org.apache.arrow.memory;
    requires arrow.memory.core;
    requires io.netty.buffer;
    requires io.netty.common;
    requires org.slf4j;
    requires org.immutables.value;
}