package lyd.ai.native4j.ck;

import java.sql.DriverManager;
import java.sql.SQLException;

public class ClickHouseDriver extends NonRegisterDriver {
    static {
        try {
            DriverManager.registerDriver(new ClickHouseDriver());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
