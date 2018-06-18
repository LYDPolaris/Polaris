package lyd.ai.native4j.ck.protocol;

import lyd.ai.native4j.ck.connect.PhysicalInfo;
import lyd.ai.native4j.ck.data.Block;
import lyd.ai.native4j.ck.serializer.BinaryDeserializer;
import lyd.ai.native4j.ck.serializer.BinarySerializer;

import java.io.IOException;
import java.sql.SQLException;

public class ExtremesResponse extends RequestOrResponse {

    private final String name;
    private final Block block;

    ExtremesResponse(String name, Block block) {
        super(ProtocolType.RESPONSE_Extremes);
        this.name = name;
        this.block = block;
    }

    @Override
    public void writeImpl(BinarySerializer serializer) throws IOException {
        throw new UnsupportedOperationException("ExtremesResponse Cannot write to Server.");
    }

    public static ExtremesResponse readFrom(BinaryDeserializer deserializer, PhysicalInfo.ServerInfo info)
        throws IOException, SQLException {
        return new ExtremesResponse(deserializer.readStringBinary(), Block.readFrom(deserializer, info));
    }
}
