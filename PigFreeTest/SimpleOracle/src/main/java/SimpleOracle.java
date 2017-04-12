import com.sun.net.httpserver.HttpContext;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;

/**
 * @author  Ruoyu Wang
 * @version 1.0
 * Date     2016.12.21
 */
public class SimpleOracle {

    private class Handler implements HttpHandler{
        public void handle(HttpExchange t) throws IOException
        {
            t.getRequestBody().close();
            String msg = "1";
            t.sendResponseHeaders(200, msg.getBytes().length);
            OutputStream out = t.getResponseBody();
            out.write(msg.getBytes());
            out.close();
        }
    }

    public void init() throws IOException
    {
        HttpServer server = HttpServer.create(new InetSocketAddress(59999), 0);
        HttpContext context = server.createContext("/", new Handler());
        server.setExecutor(null);
        server.start();
        System.out.println("Simple Oracle Started");
    }

}
