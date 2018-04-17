package ru.cubesolutions.evam.photo2rabbitmqaction;

import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.UUID;

/**
 * Created by Garya on 22.01.2018.
 */
public class PhotoStorage {

    private final static Logger log = Logger.getLogger(PhotoStorage.class);

    public String getPhotoByPersonId(UUID personId, DBConfig dbConfig) {
        try {
            Class.forName(dbConfig.getJdbcDriverClass());
            try (Connection connection = DriverManager.getConnection(dbConfig.getJdbcUrl(), dbConfig.getDbUser(), dbConfig.getDbPassword())) {
                PreparedStatement ps = connection.prepareStatement(
                        "select photo from face_detected_storage where id = ? "
                );
                ps.setString(1, personId.toString());
                ResultSet rs = ps.executeQuery();
                if (rs.next()) {
                    return rs.getString(1);
                } else {
                    throw new RuntimeException("Can't find photo by personId");
                }
            }
        } catch (Exception e) {
            log.error(e);
            throw new RuntimeException(e);
        }
    }

}
