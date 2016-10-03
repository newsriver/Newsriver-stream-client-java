package io.newsriver.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Created by eliapalme on 03/10/16.
 */
public class Client {

    private static final ObjectMapper mapper = new ObjectMapper();

    public static void main(String[] args) {

        ObjectNode payload = mapper.createObjectNode();
        payload.put("query", "text:clinton AND text:trump");
        payload.put("limit", 25);

        NewsriverStreamClient streamClient = new NewsriverStreamClient(payload.toString());

        System.out.println("=> Opening stream...");
        //openStream is blocking till the stream is closed or maximum number of reconnect is reached.
        streamClient.openStream();
    }
}
