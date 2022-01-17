
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;

import org.apache.beam.runners.direct.DirectOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;

import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;

import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;

import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;


import org.apache.beam.sdk.schemas.Schema;


import org.apache.beam.sdk.values.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Main {

    private static final Logger LOG = (Logger) LoggerFactory.getLogger(Main.class);
    /*tupleTags for side output*/
    static final TupleTag<Account> parsedMessages = new TupleTag<Account>() {
    };
    static final TupleTag<String> unparsedMessages = new TupleTag<String>() {
    };


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



    public static final Schema rawSchema = Schema
            .builder()
            .addInt32Field("id")
            .addStringField("name")
            .addStringField("surname")
            .build();


    public static PipelineResult run(MyOptions options) throws Exception {
        Pipeline pipeline = Pipeline.create(options);

        LOG.info("Building pipeline...");


        PCollection<String> message=pipeline.apply("GetDataFromPUBSub", PubsubIO.readStrings().fromSubscription(options.getSubscription()));
        PCollectionTuple transformOut =PubsubMessageToAccount.expand(message);
        PCollection<Account> accountCollection=transformOut.get(PubsubMessageToAccount.parsedMessages);
        PCollection<String> unparsedCollection=transformOut.get(PubsubMessageToAccount.unparsedMessages);

        accountCollection.apply("WriteToBigquery", BigQueryIO.<Account>write().to(options.getOutputTableName()).useBeamSchema()
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));

        unparsedCollection.apply("write to dlq",PubsubIO.writeStrings().to("projects/nttdata-c4e-bde/topics/uc1-dlq-topic-7"));


        return pipeline.run();

    }


}
