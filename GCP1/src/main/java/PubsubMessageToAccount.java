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

//converting jsonString to Account object and also differentiate the valid an invalid data
//returns tupletagPCollection with 2 tupletags first is parsedMessage for correct data and unparsedMassage for incorrect data

public  class PubsubMessageToAccount extends DoFn<String, Account> {
     

    public static TupleTag<Account> parsedMessages = new TupleTag<Account>(){};  //for correct data
    public static TupleTag<String> unparsedMessages = new TupleTag<String>(){};  //for incorrect data

    public static PCollectionTuple expand(PCollection<String> input) throws Exception {
        return input
                .apply("JsonToAccount", ParDo.of(new DoFn<String,Account>() {
                            @ProcessElement
                            public void processElement(@Element String s ,ProcessContext context) {
                                try {

                                    Gson gson = new Gson();  //craete Gson object
                                   Account a=gson.fromJson(s,Account.class); //convert jsonString Messsage to Account class
                                    context.output(parsedMessages, a); 
                                } catch (Exception e) {
                                    e.printStackTrace();  //exeception occure if data is inavlid
                                    context.output(unparsedMessages,s);
                                }

                            }
                        })
                        .withOutputTags(parsedMessages, TupleTagList.of(unparsedMessages)));


    }

}

