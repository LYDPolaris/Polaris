package lyd.ai.native4j.ck.protocol;

import lyd.ai.native4j.ck.serializer.BinaryDeserializer;
import lyd.ai.native4j.ck.serializer.BinarySerializer;

import java.io.IOException;

public class ProgressResponse extends RequestOrResponse {

    private final long newRows;
    private final long newBytes;
    private final long newTotalRows;

    public ProgressResponse(long newRows, long newBytes, long newTotalRows) {
        super(ProtocolType.RESPONSE_Progress);

        this.newRows = newRows;
        this.newBytes = newBytes;
        this.newTotalRows = newTotalRows;
    }

    @Override
    public void writeImpl(BinarySerializer serializer) throws IOException {
        throw new UnsupportedOperationException("ProgressResponse Cannot write to Server.");
    }

    public static ProgressResponse readFrom(BinaryDeserializer deserializer) throws IOException {
        return new ProgressResponse(deserializer.readVarInt(), deserializer.readVarInt(), deserializer.readVarInt());
    }

    public long getNewRows() {
        return newRows;
    }

    public long getNewBytes() {
        return newBytes;
    }

    public long getNewTotalRows() {
        return newTotalRows;
    }
}
