import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;


public  class PubsubMessageToAccount extends DoFn<String, Account> {

    public static TupleTag<Account> parsedMessages = new TupleTag<Account>(){};
    public static TupleTag<String> unparsedMessages = new TupleTag<String>(){};

    public static PCollectionTuple expand(PCollection<String> input) {
        return input
                .apply("JsonToAccount", ParDo.of(new DoFn<String,Account>() {
                            @ProcessElement
                            public void processElement(ProcessContext context) {
                                String json = context.element();
                                Gson gson = new Gson();
                                try {
                                    Account account = gson.fromJson(json, Account.class);
                                    context.output(parsedMessages, account);
                                } catch (JsonSyntaxException e) {
                                    context.output(unparsedMessages, json);
                                }

                            }
                        })
                        .withOutputTags(parsedMessages, TupleTagList.of(unparsedMessages)));


    }

}

