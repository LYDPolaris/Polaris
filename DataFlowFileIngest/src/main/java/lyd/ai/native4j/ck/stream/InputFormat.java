package lyd.ai.native4j.ck.stream;

import lyd.ai.native4j.ck.data.Block;

import java.sql.SQLException;

public interface InputFormat {
    Block next(Block header, int maxRows) throws SQLException;
}
