module arrow.memory.netty {
    exports org.apache.arrow.memory.netty;
    requires arrow.memory.core;
    requires io.netty.common;
    requires io.netty.buffer;
}