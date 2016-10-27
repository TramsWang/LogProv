package com.logprov.pipeline;

import com.logprov.Config;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;

public class EsSlave
{
    private String es_index;
    private String es_type;
    private String ess_location;
    private String mapping_json;

    private EsSlave() {}

    public EsSlave(String idx, String type, String map_file) throws IOException
    {
        es_index = idx;
        es_type = type;
        ess_location = String.format("%s://%s:%s/", Config.ESS_PROTOCOL, Config.ESS_HOST, Config.ESS_PORT);

        BufferedReader in = new BufferedReader(new FileReader(Config.ESS_MAPPING_DIR + '/' + map_file));
        String line;
        mapping_json = "";
        while (null != (line = in.readLine()))
        {
            mapping_json += line;
        }
        in.close();
        mapping_json = mapping_json.replaceAll("\\s+", "");
    }

    public void send(String json) throws IOException
    {
        System.out.println(simpleHttpRequest(ess_location + es_index + '/' + es_type, "POST", json));
    }

    public void cleanUp() throws IOException
    {
        System.out.println(simpleHttpRequest(ess_location + es_index + '/' + es_type, "DELETE", null));
    }

    public void deleteIndex() throws IOException
    {
        System.out.println(simpleHttpRequest(ess_location + es_index, "DELETE", null));
    }

    public void addIndex() throws IOException
    {
        System.out.println(simpleHttpRequest(ess_location + es_index, "PUT", null));
    }

    public void addType() throws IOException
    {
        System.out.println(simpleHttpRequest(ess_location + es_index + "/_mapping" + '/' + es_type,
                "PUT", mapping_json));
    }

    /**
     *    Send ES server some message (maybe also some files).
     *
     * @param str_url Specify the target URL.
     * @param method Http method.
     * @param content If additional file is needed, add the content string here; otherwise pass a null.
     * @return ES server response.
     * @throws IOException
     */
    public String simpleHttpRequest(String str_url, String method, String content) throws IOException
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
        System.out.println("ES Resp: " + Integer.toString(resp_code));
        return resp;
    }

    public String getESSLocation() {return ess_location + es_index + '/' + es_type + '/';}
}
