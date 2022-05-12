module arrow.vector {
    exports org.apache.arrow.vector;
    exports org.apache.arrow.vector.compression;
    exports org.apache.arrow.vector.dictionary;
    exports org.apache.arrow.vector.ipc;
    exports org.apache.arrow.vector.ipc.message;
    exports org.apache.arrow.vector.types;
    exports org.apache.arrow.vector.types.pojo;
    exports org.apache.arrow.vector.validate;
    requires arrow.memory.core;
    requires org.apache.arrow.flatbuf;
    requires com.fasterxml.jackson.databind;
    requires io.netty.common;
    requires java.sql;
    requires commons.codec;
    requires com.fasterxml.jackson.datatype.jsr310;
//    requires org.slf4j;
}