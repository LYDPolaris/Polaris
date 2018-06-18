package lyd.ai.native4j.ck;

import lyd.ai.native4j.ck.data.Block;
import lyd.ai.native4j.ck.data.IDataType;
import lyd.ai.native4j.ck.data.type.complex.DataTypeNullable;
import lyd.ai.native4j.ck.wrapper.SQLResultSetMetaData;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class ClickHouseResultSetMetaData extends SQLResultSetMetaData {

    private final Block header;

    public ClickHouseResultSetMetaData(Block header) {
        this.header = header;
    }

    @Override
    public int getColumnCount() throws SQLException {
        return header.columns();
    }

    @Override
    public int getColumnType(int index) throws SQLException {
        IDataType type = header.getByPosition(index - 1).type();
        return type.sqlTypeId();
    }

    @Override
    public String getColumnName(int index) throws SQLException {
        return getColumnLabel(index);
    }

    @Override
    public String getColumnLabel(int index) throws SQLException {
        return header.getByPosition(index - 1).name();
    }

    @Override
    public int isNullable(int index) throws SQLException {
        return (header.getByPosition(index - 1).type() instanceof DataTypeNullable) ?
            ResultSetMetaData.columnNullable : ResultSetMetaData.columnNoNulls;
    }

    @Override
    public boolean isSigned(int index) throws SQLException {
        return header.getByPosition(index).name().startsWith("U");
    }


    /*=========================================================*/

    @Override
    public boolean isReadOnly(int index) throws SQLException {
        return true;
    }

    @Override
    public boolean isWritable(int index) throws SQLException {
        return false;
    }

    @Override
    public boolean isAutoIncrement(int index) throws SQLException {
        return false;
    }
}
