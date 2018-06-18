package lyd.ai.native4j.ck.buffer;

import java.io.IOException;

public interface BuffedReader {
    int readBinary() throws IOException;

    int readBinary(byte[] bytes) throws IOException;
}
