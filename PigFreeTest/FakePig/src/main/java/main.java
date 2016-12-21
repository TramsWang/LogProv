import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;

/**
 * Created by tramswang on 16-12-21.
 */
public class main {
    public static void main(String[] args) throws Exception
    {
        String pid = simpleHttpRequest("http://master:58888/_start", "GET", null);
        Thread t = new Thread();
        t.sleep(2000);
        System.out.println("---------------------------------------------\n");

        /* Simulate the pipeline execution */
        BufferedReader file_in = new BufferedReader(new FileReader("config.txt"));
        String line;
        while (null != (line = file_in.readLine()))
        {
            if ("".equals(line)) continue;
            String paras[] = line.split(";"); //srcvar, operation, dstvar
            simpleHttpRequest("http://master:58888/_reqDS", "GET",
                    String.format("%s\n%s\n%s\n%s", pid, paras[0], paras[1], paras[2]));
        }
        System.out.println("---------------------------------------------\n");
        file_in.close();
        t.sleep(2000);

        /* Evaluate before termination */
        file_in = new BufferedReader(new FileReader("EvalBeforeTerm.txt"));
        while (null != (line = file_in.readLine()))
        {
            simpleHttpRequest("http://master:58888/_eval", "GET",
                    String.format("%s\n%s\n%s", pid, line, "http://slave2:59999"));
            t.sleep(2000);
        }
        file_in.close();
        System.out.println("---------------------------------------------\n");

        /* Terminate pipeline execution */
        simpleHttpRequest("http://master:58888/_terminate", "GET", pid);
        System.out.println("---------------------------------------------\n");
        t.sleep(2000);

        /* Evaluate after pipeline termination */
        file_in = new BufferedReader(new FileReader("EvalAfterTerm.txt"));
        while (null != (line = file_in.readLine()))
        {
            simpleHttpRequest("http://master:58888/_eval", "GET",
                    String.format("%s\n%s\n%s", pid, line, "http://slave2:59999"));
            t.sleep(2000);
        }
        file_in.close();
        System.out.println("---------------------------------------------\n");

        /* Fetch pipeline meta info */
        PrintWriter file_out = new PrintWriter("result.csv");
        simpleHttpRequest("http://master:58888/_search", "GET", String.format("meta\n%s", pid), file_out);
        file_out.close();
        System.out.println("Write to: 'result.csv'");
    }

    public static String simpleHttpRequest(String str_url, String method, String content) throws IOException
    {
        URL url = new URL(str_url);
        HttpURLConnection con = (HttpURLConnection)url.openConnection();
        con.setRequestMethod(method);
        con.setDoInput(true);
        if (null == content)
        {
            con.setDoOutput(false);
        }
        else
        {
            con.setDoOutput(true);
            OutputStream out = con.getOutputStream();
            out.write(content.getBytes());
            out.close();
        }

        int resp_code = con.getResponseCode();
        BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
        String inputline;
        String resp = "";
        while (null != (inputline = in.readLine()))
        {
            resp += inputline;
        }
        System.out.println(String.format("Response Code: %d", resp_code));
        System.out.println(String.format("Response: %s", resp));
        return resp;
    }

    public static void simpleHttpRequest(String str_url, String method, String content, PrintWriter file) throws IOException
    {
        URL url = new URL(str_url);
        HttpURLConnection con = (HttpURLConnection)url.openConnection();
        con.setRequestMethod(method);
        con.setDoInput(true);
        if (null == content)
        {
            con.setDoOutput(false);
        }
        else
        {
            con.setDoOutput(true);
            OutputStream out = con.getOutputStream();
            out.write(content.getBytes());
            out.close();
        }

        int resp_code = con.getResponseCode();
        BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
        String inputline;
        while (null != (inputline = in.readLine()))
        {
            file.println(inputline);
        }
        System.out.println(String.format("Response Code: %d", resp_code));
    }
}
