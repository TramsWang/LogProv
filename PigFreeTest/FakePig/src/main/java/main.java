import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;

/**
 * Created by tramswang on 16-12-21.
 */
public class main {
    public static void main(String[] args) throws Exception
    {
        BufferedReader in = new BufferedReader(new FileReader("config.txt"));
        String line;
        Tester tester;

        while (null != (line = in.readLine()))
        {
            tester = new Tester(line);
            tester.test();
        }
    }
}
