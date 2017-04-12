import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;

/**
 * @author  Ruoyu Wang
 * @version 1.0
 * Date     2017.04.12
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
