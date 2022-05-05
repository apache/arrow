module arrow.memory.core {
    exports org.apache.arrow.memory;
    exports org.apache.arrow.util;
    requires org.slf4j;
    requires org.immutables.value;
    requires jsr305;
    requires jdk.unsupported;
}