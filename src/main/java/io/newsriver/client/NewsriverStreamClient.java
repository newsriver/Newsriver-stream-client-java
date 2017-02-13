package io.newsriver.client;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.log4j.Logger;
import org.glassfish.tyrus.client.ClientManager;

import javax.websocket.ClientEndpoint;
import javax.websocket.CloseReason;
import javax.websocket.DeploymentException;
import javax.websocket.OnClose;
import javax.websocket.OnMessage;
import javax.websocket.OnOpen;
import javax.websocket.PongMessage;
import javax.websocket.Session;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Created by eliapalme on 03/09/16.
 */
@ClientEndpoint(configurator = NewsriverStreamClient.AuthClientConfigurator.class)
public class NewsriverStreamClient {

    private static final String ACCESS_TOKEN = "--YOUR-API-TOKEN-HERE--";
    private static final Logger logger = Logger.getLogger(NewsriverStreamClient.class);
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final int NUMBER_OF_RECONNECT_RETRIES = 50;
    private static final int PING_TIMEOUT = 60000; //1 min
    private static final int RECONNECT_GRACE_PERIOD = 2500; //2.5 sec
    private static CountDownLatch latchReconnect;
    private String query;

    public NewsriverStreamClient(String query){
        this.query=query;
    }

    public void openStream() {

        latchReconnect = new CountDownLatch(NUMBER_OF_RECONNECT_RETRIES);
        ClientManager client = ClientManager.createClient();
        ClientManager.ReconnectHandler reconnectHandler = new ClientManager.ReconnectHandler() {

            //Always re-connect
            @Override
            public boolean onDisconnect(CloseReason closeReason) {
                return true;
            }

            @Override
            public boolean onConnectFailure(Exception exception) {
                logger.warn(String.format("Stream connection failure %s", exception));
                try {
                    latchReconnect.countDown();
                    if (latchReconnect.await(RECONNECT_GRACE_PERIOD, TimeUnit.MILLISECONDS)) {
                        return false;
                    } else {
                        return true;
                    }
                } catch (InterruptedException ex) {
                    logger.warn("Unable to recconect", ex);
                }
                return false;
            }
        };

        client.getProperties().put(ClientManager.RECONNECT_HANDLER, reconnectHandler);
        client.setDefaultMaxSessionIdleTimeout(PING_TIMEOUT);
        client.setAsyncSendTimeout(PING_TIMEOUT);
        try {
            client.connectToServer(this, new URI("wss://api.newsriver.io/v2/search-stream"));
            latchReconnect.await();
        } catch (DeploymentException | URISyntaxException | InterruptedException | IOException ex) {
            logger.fatal("Unable to connect", ex);
        }
    }

    @OnOpen
    public void onOpen(Session session) {
        try {
            session.getBasicRemote().sendPing(null);
            session.getBasicRemote().sendText(this.query);
        } catch (IOException ex) {
            logger.warn("Unable to send message", ex);
        }
    }

    @OnMessage
    public void onMessage(String message, Session session) {

        String[] lines = message.split(System.getProperty("line.separator"));
        //Every line of the message stream correspond to a single article
        for (String line : lines) {
            try {
                JsonNode article = mapper.readTree(line);
                //TODO: put your code here
                //---------------------------
                System.out.println(article.get("title").asText());
                //---------------------------

            } catch (Exception ex) {
                logger.warn("Unable to deserialize message", ex);
            }
        }
    }

    @OnMessage
    public void onPong(PongMessage pongMessage, Session session) {
        try {
            latchReconnect.await(PING_TIMEOUT / 2, TimeUnit.MILLISECONDS);
            session.getBasicRemote().sendPing(null);
        } catch (IOException ex) {
            logger.warn("Unable to send ping", ex);
        } catch (InterruptedException ex) {
            logger.warn("Unable to recconect", ex);
        }
    }

    @OnClose
    public void onClose(Session session, CloseReason closeReason) {
        logger.info(String.format("Stream %s close because of %s", session.getId(), closeReason));
    }
    
    public static class AuthClientConfigurator extends ClientEndpointConfig.Configurator {
        @Override
        public void beforeRequest(Map<String, List<String>> headers) {
            headers.put("Authorization", Arrays.asList(ACCESS_TOKEN));
        }
    }

}
