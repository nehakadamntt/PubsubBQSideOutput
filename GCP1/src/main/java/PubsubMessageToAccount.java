import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;



public  class PubsubMessageToAccount extends DoFn<String, Account> {
     

    public static TupleTag<Account> parsedMessages = new TupleTag<Account>(){};
    public static TupleTag<String> unparsedMessages = new TupleTag<String>(){};

    public static PCollectionTuple expand(PCollection<String> input) throws Exception {
        return input
                .apply("JsonToAccount", ParDo.of(new DoFn<String,Account>() {
                            @ProcessElement
                            public void processElement(@Element String s ,ProcessContext context) {
                                try {

                                    Gson gson = new Gson();
                                   Account a=gson.fromJson(s,Account.class);
                                    context.output(parsedMessages, a);
                                } catch (Exception e) {
                                    e.printStackTrace();
                                    context.output(unparsedMessages,s);
                                }

                            }
                        })
                        .withOutputTags(parsedMessages, TupleTagList.of(unparsedMessages)));


    }

}

