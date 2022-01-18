import com.google.gson.Gson;
import com.google.pubsub.v1.ProjectSubscriptionName;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;

import org.apache.beam.runners.direct.DirectOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;

import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;

import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.schemas.Schema;

import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.*;
import com.google.gson.JsonSyntaxException;

import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.*;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import java.util.Arrays;


public class Main {

    private static final Logger LOG = (Logger) LoggerFactory.getLogger(Main.class);
    /*tupleTags for side output*/
    static final TupleTag<Account> parsedMessages = new TupleTag<Account>() {
    };
    static final TupleTag<String> unparsedMessages = new TupleTag<String>() {
    };

    /*
     * class provides the custom execution options passed by the
     * executor at the command-line.
     * */
    public interface MyOptions extends DataflowPipelineOptions, DirectOptions {

        @Description("BigQuery table name")
        String getOutputTableName();
        void setOutputTableName(String outputTableName);

        @Description("PubSub Subscription")
        String getSubscription();
        void setSubscription(String subscription);



        

    }


    public static void main(String[] args) throws Exception {


        PipelineOptionsFactory.register(MyOptions.class);
        //Setting option parameter
        MyOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(MyOptions.class);

        System.out.println(options.getSubscription());
        run(options);


    }




     /*
     * create schema same like outputtable schema 
     * It is used while conerting json string to row using JsonToRow transformation
     * */

    public static final Schema rawSchema = Schema
            .builder()
            .addInt64Field("id")
            .addStringField("name")
            .addStringField("surname")
            .build();
    
      

    public static PipelineResult run(MyOptions options) throws Exception{
        //creating pipeline
        Pipeline pipeline = Pipeline.create(options);

        LOG.info("Building pipeline...");

        //reading message as string from pubsub using subscription
        PCollection<String> message=pipeline.apply("GetDataFromPUBSub", PubsubIO.readStrings().fromSubscription(options.getSubscription()));
        
        //convert jsonstring pubsub message to Account type object. 
        //expand Function will return tupletag pcollection
        PCollectionTuple transformOut =PubsubMessageToAccount.expand(message);
        
        //getting correct or parsedMessage using above tupletagCollection
        PCollection<Account> accountCollection=transformOut.get(PubsubMessageToAccount.parsedMessages);
        
        //getting erraneous message
        PCollection<String> unparsedCollection=transformOut.get(PubsubMessageToAccount.unparsedMessages);
        
        
        //for correct data
        //this transformation convert Account object to jsonString
        accountCollection.apply(ParDo.of(new DoFn<Account,String>() {
            @ProcessElement
            public void processElement( ProcessContext context)
            {
                Gson g=new Gson();
                String s=g.toJson(context.element());
                 context.output(s);
            }

        }))
            //convert input json string to row using schema
            .apply(JsonToRow.withSchema(rawSchema))
            //write to Bigquery table
            .apply("WriteToBigquery", BigQueryIO.<Row>write(). 
                to(options.getOutputTableName()).useBeamSchema()
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));


        
       
        //for erraneous data 
        //store it in DLQ topic
        unparsedCollection.apply("write to dlq",PubsubIO.writeStrings().to("path_to_dlq"));

        //run pipeline
        return pipeline.run();

    }


}
