import Utils.Addition;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.VoidHandler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.web.Route;
import io.vertx.ext.web.Router;

import org.cinchapi.concourse.Concourse;

import java.io.InputStream;
import java.util.logging.LogManager;

/**
 * Created by akshay.kumar1 on 20/07/16.
 */
public class VertxController extends AbstractVerticle {

    private HttpServer server = null;
    private Logger logger;

    private final Concourse concourse = Concourse.connect();

    @Override
    public void start(Future<Void> startFuture) throws Exception {

        super.start(startFuture);

        // Initialize the server
        server = vertx.createHttpServer();

        // Create a router object.
        Router router = Router.router(vertx);

        //Initialize the logger
        initLogger();

        logger.info("Vertx Started");

        // This is a GET API. Call this API "/getUrl?key=Hello&record=1"
        // curl -X GET -H "Content-Type: application/json"  "http://127.0.0.1:8090/getUrl?key=abc&record=1"
        Route getRoute = router.route(HttpMethod.GET, config().getString("get_url"));
        getRoute.handler(routingContext -> {
            HttpServerRequest request = routingContext.request();
            String key = request.params().get("key");
            String recordString = request.params().get("record");
            logger.info("GET call with key = "+key+" and record = "+recordString);
            int sum = -1;
            if(isNullOrEmpty(key) || isNullOrEmpty(recordString)) {
                logger.info("Empty key or Record");
            } else {
                long record = Long.parseLong(recordString);
                sum = Addition.INSTANCE.retrive(concourse, key, record);
            }
            request.response().end(createResponse(String.valueOf(sum)));
            logger.info("Done with GET call");
        });


        // This is a POST API. Call this API "/postUrl"
        // curl -X POST -H "Content-Type: application/json" -d '{"key":"abc", "value1":2, "value2":6, "record":1}' "http://127.0.0.1:8090/postUrl"
        Route postRoute = router.route(HttpMethod.POST, config().getString("post_url"));
        postRoute.handler(routingContext -> {
            HttpServerRequest request = routingContext.request();
            final Buffer body = Buffer.buffer();

            request.handler(new Handler<Buffer>() {
                public void handle(Buffer buffer) {
                    body.appendBuffer(buffer);
                }
            });

            request.endHandler(new VoidHandler() {
                public void handle() {
                    String requestBody = body.getString(0, body.length(), "UTF-8");
                    JsonObject jsonRequest = new JsonObject(requestBody);
                    String key = jsonRequest.getString("key");
                    int value1 = jsonRequest.getInteger("value1");
                    int value2 = jsonRequest.getInteger("value2");
                    long record = jsonRequest.getLong("record");
                    logger.info("POST call with para = "+jsonRequest.toString());

                    String response = "";
                    if(isNullOrEmpty(key)) {
                        response = "Empty Input";
                    } else {
                        if(Addition.INSTANCE.add(value1, value2, concourse, key, record)) {
                            response = "Success";
                        } else {
                            response = "Failure";
                        }

                    }
                    request.response().end(createResponse(response));
                    logger.info("Done with POST");
                }
            });
        });

        server.requestHandler(router::accept).listen(config().getInteger("server_port"));
    }

    @Override
    public void stop(Future<Void> stopFuture) throws Exception {
        // TODO Auto-generated method stub
        super.stop(stopFuture);
    }

    private String createResponse(String input) {
        JsonObject json = new JsonObject();
        json.put("response", input);
        return (json.toString());
    }

    private boolean isNullOrEmpty(String input) {
        return (null == input || input.isEmpty());
    }

    private void initLogger(){
        try {
            final InputStream inputStream = VertxController.class.getResourceAsStream("/logging.properties");
            LogManager.getLogManager().readConfiguration(inputStream);
            logger = (Logger) LoggerFactory.getLogger(VertxController.class.getName());
            logger.info("Logger initialized");
        } catch(Exception ex) {

        }
    }
}
