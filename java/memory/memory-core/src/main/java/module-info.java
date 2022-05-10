module arrow.memory.core {
    exports org.apache.arrow.memory;
    exports org.apache.arrow.memory.rounding;
    exports org.apache.arrow.util;
    exports org.apache.arrow.memory.util;
//    opens java.nio;
    exports org.apache.arrow.memory.util.hash;
    requires org.slf4j;
    requires org.immutables.value;
    requires jsr305;
    requires jdk.unsupported;
}