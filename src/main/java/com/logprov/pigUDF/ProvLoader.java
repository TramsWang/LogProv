package com.logprov.pigUDF;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.*;

import com.google.gson.Gson;
import com.logprov.Config;
import com.logprov.pipeline.LogLine;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.pig.Expression;
import org.apache.pig.FileInputLoadFunc;
import org.apache.pig.LoadCaster;
import org.apache.pig.LoadFunc;
import org.apache.pig.LoadMetadata;
import org.apache.pig.LoadPushDown;
import org.apache.pig.OverwritableStoreFunc;
import org.apache.pig.PigConfiguration;
import org.apache.pig.PigException;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.ResourceStatistics;
import org.apache.pig.StoreFunc;
import org.apache.pig.StoreFuncInterface;
import org.apache.pig.StoreMetadata;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MRConfiguration;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigTextInputFormat;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigTextOutputFormat;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.bzip2r.Bzip2TextInputFormat;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.util.CastUtils;
import org.apache.pig.impl.util.ObjectSerializer;
import org.apache.pig.impl.util.StorageUtil;
import org.apache.pig.impl.util.UDFContext;
import org.apache.pig.impl.util.Utils;
import org.apache.pig.parser.ParserException;

/**
 * Created by:  Ruoyu Wang
 * Date:        2017.04.05
 *
 *   LogProv loader class. Load data, assign pipeline execution ID and tell PM about the loading
 * specification.
 *
 * TODO: Combine Load and Store into one class.
 * TODO: Modify usage and comments for adding two new parameters[TEST]
 */
public class ProvLoader extends LoadFunc {

    protected RecordReader in = null;
    protected final Log mLog = LogFactory.getLog(getClass());
    protected String signature;

    private byte fieldDel = '\t';
    private ArrayList<Object> mProtoTuple = null;
    private TupleFactory mTupleFactory = TupleFactory.getInstance();
    private String loadLocation;

    boolean isSchemaOn = false;
    boolean dontLoadSchema = false;
    boolean overwriteOutput = false;
    protected ResourceSchema schema;
    protected LoadCaster caster;

    protected boolean[] mRequiredColumns = null;
    private boolean mRequiredColumnsInitialized = false;

    // Indicates whether the input file name/path should be read.
    private boolean tagFile = false;
    private static final String TAG_SOURCE_FILE = "tagFile";
    private boolean tagPath = false;
    private static final String TAG_SOURCE_PATH = "tagPath";
    private Path sourcePath = null;

    // it determines whether to depend on pig's own Bzip2TextInputFormat or
    // to simply depend on hadoop for handling bzip2 inputs
    private boolean bzipinput_usehadoops ;

    /* Paras for LogProv only */
    private String pipeline_monitor_location = null;
    private String hdfspath = null;
    private String info = null;
    private LogLine log = new LogLine();

    //--------------------------------------------------------------------------------------------------------------//
    private Options populateValidOptions() {
        Options validOptions = new Options();
        validOptions.addOption("schema", false, "Loads / Stores the schema of the relation using a hidden JSON file.");
        validOptions.addOption("noschema", false, "Disable attempting to load data schema from the filesystem.");
        validOptions.addOption(TAG_SOURCE_FILE, false, "Appends input source file name to beginning of each tuple.");
        validOptions.addOption(TAG_SOURCE_PATH, false, "Appends input source file path to beginning of each tuple.");
        validOptions.addOption("tagsource", false, "Appends input source file name to beginning of each tuple.");
        Option overwrite = new Option("overwrite", "Overwrites the destination.");
        overwrite.setLongOpt("overwrite");
        overwrite.setOptionalArg(true);
        overwrite.setArgs(1);
        overwrite.setArgName("overwrite");
        validOptions.addOption(overwrite);

        return validOptions;
    }

    /* Overloaded version */
    public ProvLoader(String dstvar, String column_type, String inspected_columns,
                      String pm_location, String hdfspath, String info) throws IOException
    {
        this(dstvar, pm_location, column_type, inspected_columns, hdfspath, info, ",", "");
    }

    /* Initialize parameters for later use */
    public ProvLoader(String dstvar, String column_type, String inspected_columns,
                      String pm_location, String hdfspath, String info, String delimiter, String options)
            throws IOException {
        this.log.dstvar = dstvar;
        this.log.column_type = column_type;
        this.log.inspected_columns = inspected_columns;
        this.pipeline_monitor_location = pm_location;
        this.hdfspath = hdfspath;
        this.info = info;

        fieldDel = StorageUtil.parseFieldDel(delimiter);
        Options validOptions = populateValidOptions();
        String[] optsArr = options.split(" ");
        try {
            CommandLineParser parser = new GnuParser();
            CommandLine configuredOptions = parser.parse(validOptions, optsArr);
            isSchemaOn = configuredOptions.hasOption("schema");
            if (configuredOptions.hasOption("overwrite")) {
                String value = configuredOptions.getOptionValue("overwrite");
                if ("true".equalsIgnoreCase(value)) {
                    overwriteOutput = true;
                }
            }
            dontLoadSchema = configuredOptions.hasOption("noschema");
            tagFile = configuredOptions.hasOption(TAG_SOURCE_FILE);
            tagPath = configuredOptions.hasOption(TAG_SOURCE_PATH);
            if (configuredOptions.hasOption("tagsource")) {
                mLog.warn("'-tagsource' is deprecated. Use '-tagFile' instead.");
                tagFile = true;
            }
        } catch (ParseException e) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp( "PigStorage(',', '[options]')", validOptions);
            // We wrap this exception in a Runtime exception so that
            // existing loaders that extend PigStorage don't break
            throw new RuntimeException(e);
        }
    }

    private Tuple applySchema(Tuple tup) throws IOException {
        if ( caster == null) {
            caster = getLoadCaster();
        }
        if (signature != null && schema == null) {
            Properties p = UDFContext.getUDFContext().getUDFProperties(this.getClass(),
                    new String[] {signature});
            String serializedSchema = p.getProperty(signature+".schema");
            if (serializedSchema == null) return tup;
            try {
                schema = new ResourceSchema(Utils.getSchemaFromString(serializedSchema));
            } catch (ParserException e) {
                mLog.error("Unable to parse serialized schema " + serializedSchema, e);
                // all bets are off - there's no guarantee that we'll return
                // either the fields in the data or the fields in the schema
                // the user specified (or required)
            }
        }

        if (schema != null) {
            ResourceFieldSchema[] fieldSchemas = schema.getFields();
            int tupleIdx = 0;
            // If some fields have been projected out, the tuple
            // only contains required fields.
            // We walk the requiredColumns array to find required fields,
            // and cast those.
            for (int i = 0; i < fieldSchemas.length; i++) {
                if (mRequiredColumns == null || (mRequiredColumns.length>i && mRequiredColumns[i])) {
                    if (tupleIdx >= tup.size()) {
                        tup.append(null);
                    }

                    Object val = null;
                    if(tup.get(tupleIdx) != null){
                        byte[] bytes = ((DataByteArray) tup.get(tupleIdx)).get();
                        val = CastUtils.convertToType(caster, bytes,
                                fieldSchemas[i], fieldSchemas[i].getType());
                        tup.set(tupleIdx, val);
                    }
                    tupleIdx++;
                }
            }
        }
        return tup;
    }

    protected DataByteArray readField(byte[] bytes, int start, int end) {
        if (start == end) {
            return null;
        } else {
            return new DataByteArray(bytes, start, end);
        }
    }

    private void addTupleValue(ArrayList<Object> tuple, byte[] buf, int start, int end) {
        tuple.add(readField(buf, start, end));
    }

    /*
     *   Lazy method to connect to PM and acquire PID.
     *
     */
    public Tuple getNext() throws IOException
    {
        /* First execution, check pid and tell PM about loading operation */
        if (null == log.pid)
        {
            /* Get HDFS connection */
            String hd_conf_dir = System.getenv("HADOOP_CONF_DIR");
            if (null == hd_conf_dir)
                throw new IOException("Environment variable 'HADOOP_CONF_DIR' not set!!");
            Configuration conf = new Configuration();
            conf.addResource(new Path(hd_conf_dir + "/core-site.xml"));
            conf.addResource(new Path(hd_conf_dir + "/hdfs-site.xml"));
            conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
            conf.set("fs.file.impl", LocalFileSystem.class.getName());
            System.out.println("LOADER:: Connecting to: " + conf.get("fs.defaultFS"));
            FileSystem hdfs = FileSystem.get(conf);

            /* Check PID file and read PID */
            if (!hdfs.exists(new Path(Config.PID_FILE)))
            {
                /* No PID assigned, require a new one */
                URL url = new URL(String.format("%s%s", pipeline_monitor_location, Config.CONT_START_PIPELINE));
                HttpURLConnection con = (HttpURLConnection)url.openConnection();
                con.setRequestMethod("PUT");
                con.setDoInput(true);
                con.setDoOutput(true);
                OutputStream out = con.getOutputStream();
                out.write((hdfspath+'\n').getBytes());
                out.write(info.getBytes());
                con.getOutputStream().close();
                int respcode = con.getResponseCode();
                if (400 == respcode)
                    throw new IOException("LOADER:: PM PUT Error!");
                BufferedReader reader = new BufferedReader(new InputStreamReader(con.getInputStream()));
                log.pid = reader.readLine();
                reader.close();

                PrintWriter writer = new PrintWriter(hdfs.create(new Path(Config.PID_FILE)));
                writer.println(log.pid);
                writer.flush();
                System.out.println("LOADER::<<NEW PID>>:"+log.pid);
                writer.close();
            }
            else
            {
                /* Already got a PID, read */
                BufferedReader reader = new BufferedReader(new InputStreamReader(hdfs.open(new Path(Config.PID_FILE))));
                log.pid = reader.readLine();
                reader.close();
            }

            /* Tell PM about the load operation */
            URL url = new URL(String.format("%s%s", pipeline_monitor_location, Config.CONT_REQUIRE_DATA_STORAGE));
            HttpURLConnection con = (HttpURLConnection)url.openConnection();
            con.setRequestMethod("PUT");
            con.setDoInput(true);
            con.setDoOutput(true);
            OutputStream out = con.getOutputStream();
            log.srcvar = null;
            log.srcidx = null;
            log.operation = String.format("LOADER_%s", log.dstvar);
            log.start = new Date().toString();
            out.write(new Gson().toJson(log).getBytes());  /* 'pid', 'srcvar', 'operation' and 'start' set above
                                                            * 'dstvar' already set in 'ProvLoader'
                                                            * 'srcidx' and 'dstidx' already set in 'setLocation'*/
            out.close();
            int respcode = con.getResponseCode();
            if (400 == respcode)
                throw new IOException("LOADER:: PM PUT Error!");
            BufferedReader reader = new BufferedReader(new InputStreamReader(con.getInputStream()));
            System.out.println(String.format("LOADER::<<REQ_DS>>:%s", reader.readLine()));
            reader.close();
        }

        mProtoTuple = new ArrayList<Object>();
        if (!mRequiredColumnsInitialized) {
            if (signature!=null) {
                Properties p = UDFContext.getUDFContext().getUDFProperties(this.getClass());
                mRequiredColumns = (boolean[])ObjectSerializer.deserialize(p.getProperty(signature));
            }
            mRequiredColumnsInitialized = true;
        }
        //Prepend input source path if source tagging is enabled
        if(tagFile) {
            mProtoTuple.add(new DataByteArray(sourcePath.getName()));
        } else if (tagPath) {
            mProtoTuple.add(new DataByteArray(sourcePath.toString()));
        }

        try {
            boolean notDone = in.nextKeyValue();
            if (!notDone) {
                return null;
            }
            Text value = (Text) in.getCurrentValue();
            byte[] buf = value.getBytes();
            int len = value.getLength();
            int start = 0;
            int fieldID = 0;
            for (int i = 0; i < len; i++) {
                if (buf[i] == fieldDel) {
                    if (mRequiredColumns==null || (mRequiredColumns.length>fieldID && mRequiredColumns[fieldID]))
                        addTupleValue(mProtoTuple, buf, start, i);
                    start = i + 1;
                    fieldID++;
                }
            }
            // pick up the last field
            if (start <= len && (mRequiredColumns==null || (mRequiredColumns.length>fieldID && mRequiredColumns[fieldID]))) {
                addTupleValue(mProtoTuple, buf, start, len);
            }
            Tuple t =  mTupleFactory.newTupleNoCopy(mProtoTuple);

            return dontLoadSchema ? t : applySchema(t);
        } catch (InterruptedException e) {
            int errCode = 6018;
            String errMsg = "Error while reading input";
            throw new ExecException(errMsg, errCode,
                    PigException.REMOTE_ENVIRONMENT, e);
        }
    }

    @Override
    public InputFormat getInputFormat() {
        if((loadLocation.endsWith(".bz2") || loadLocation.endsWith(".bz"))
                && (!bzipinput_usehadoops) ) {
            mLog.info("Using Bzip2TextInputFormat");
            return new Bzip2TextInputFormat();
        } else {
            mLog.info("Using PigTextInputFormat");
            return new PigTextInputFormat();
        }
    }

    @Override
    public void prepareToRead(RecordReader reader, PigSplit split) {
        in = reader;
        if (tagFile || tagPath) {
            sourcePath = ((FileSplit)split.getWrappedSplit()).getPath();
        }
    }

    @Override
    public void setLocation(String location, Job job)
            throws IOException {
        loadLocation = location;
        log.dstidx = loadLocation;
        FileInputFormat.setInputPaths(job, location);
        bzipinput_usehadoops = job.getConfiguration().getBoolean(
                PigConfiguration.PIG_BZIP_USE_HADOOP_INPUTFORMAT,
                true );
    }

    public String relativeToAbsolutePath(String location, Path curDir){return location;}

}
