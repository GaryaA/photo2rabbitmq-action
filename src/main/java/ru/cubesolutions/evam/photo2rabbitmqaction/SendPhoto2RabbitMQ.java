package ru.cubesolutions.evam.photo2rabbitmqaction;

import com.intellica.evam.sdk.outputaction.AbstractOutputAction;
import com.intellica.evam.sdk.outputaction.IOMParameter;
import com.intellica.evam.sdk.outputaction.OutputActionContext;
import org.apache.log4j.Logger;
import ru.cubesolutions.rabbitmq.Producer;
import ru.cubesolutions.rabbitmq.RabbitConfig;

import java.io.FileInputStream;
import java.io.InputStream;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Properties;
import java.util.UUID;

/**
 * Created by Garya on 18.01.2018.
 */
public class SendPhoto2RabbitMQ extends AbstractOutputAction {

    private final static Logger log = Logger.getLogger(SendPhoto2RabbitMQ.class);
    private final static DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("HH:mm:ss dd.MM.yyyy");

    private Producer producer;
    private DBConfig dbConfig;
    private String exchange;
    private String routingKey;

    @Override
    public synchronized void init() {
        isInited = false;
        try (InputStream input = new FileInputStream("./conf/rabbitmq.properties")) {
            Properties props = new Properties();
            props.load(input);
            String host = props.getProperty("host");
            int port = Integer.parseInt(props.getProperty("port"));
            String vHost = props.getProperty("v-host");
            String user = props.getProperty("user");
            String password = props.getProperty("password");
            this.exchange = props.getProperty("exchange") == null ? "" : props.getProperty("exchange");
            this.routingKey = props.getProperty("routing-key");
            RabbitConfig rabbitConfig = new RabbitConfig(host, port, vHost, user, password);
            this.producer = new Producer(rabbitConfig);
        } catch (Throwable t) {
            log.error("failed to init SendPhoto2RabbitMQ action", t);
            isInited = false;
            return;
        }
        try (InputStream input = new FileInputStream("./conf/photo-storage.properties")) {
            Properties props = new Properties();
            props.load(input);
            String jdbcDriverClass = props.getProperty("jdbc-driver-class");
            String jdbcUrl = props.getProperty("jdbc-url");
            String dbUser = props.getProperty("db-user");
            String dbPassword = props.getProperty("db-password");
            this.dbConfig = new DBConfig(jdbcDriverClass, jdbcUrl, dbUser, dbPassword);
        } catch (Throwable t) {
            log.error("failed to init SendPhoto2RabbitMQ action", t);
            isInited = false;
            return;
        }
        isInited = true;
    }

    @Override
    public int execute(OutputActionContext outputActionContext) throws Exception {
        UUID personId = UUID.fromString((String) outputActionContext.getParameter("person_id"));
        log.debug("person_id is " + personId);
        String name = (String) outputActionContext.getParameter("name");
        log.debug("name is " + name);

        String scenarioName = (String) outputActionContext.getParameter("scenario_name");
        log.debug("scenario_name is " + scenarioName);

        PhotoStorage photoStorage = new PhotoStorage();
        String photoBase64 = photoStorage.getPhotoByPersonId(personId, this.dbConfig);
        log.debug("photoBase64 is " + photoBase64);

        this.producer.sendMessage(formatEventJson(personId, name, photoBase64, scenarioName), this.exchange, this.routingKey);
        return 0;
    }

    private static String formatEventJson(UUID personId, String name, String photoBase64, String scenarioName) {
        String currentTimestampFormatted = LocalDateTime.now().format(DATE_TIME_FORMATTER);
        return String.format(eventJsonStructure(), personId, name, photoBase64, currentTimestampFormatted, scenarioName);
    }

    private static String eventJsonStructure() {
        return "{\"id\":\"%s\"," +
                "\"name\":\"%s\"," +
                "\"photo\":\"%s\"," +
                "\"timestamp\":\"%s\"," +
                "\"scenario\":\"%s\"" +
                "}";
    }

    @Override
    protected ArrayList<IOMParameter> getParameters() {
        ArrayList<IOMParameter> params = new ArrayList<>();
        params.add(new IOMParameter("person_id", "person_id"));
        params.add(new IOMParameter("name", "name"));
        params.add(new IOMParameter("scenario_name", "scenario_name"));
        return params;
    }

    @Override
    public String getVersion() {
        return "1.0";
    }
}
