package lyd.ai.native4j.ck.protocol;

import lyd.ai.native4j.ck.serializer.BinaryDeserializer;
import lyd.ai.native4j.ck.serializer.BinarySerializer;

import java.io.IOException;

public class EOFStreamResponse extends RequestOrResponse {

    EOFStreamResponse() {
        super(ProtocolType.RESPONSE_EndOfStream);
    }

    @Override
    public void writeImpl(BinarySerializer serializer) throws IOException {
        throw new UnsupportedOperationException("EndOfStreamResponse Cannot write to Server.");
    }

    public static RequestOrResponse readFrom(BinaryDeserializer deserializer) {
        return new EOFStreamResponse();
    }
}
