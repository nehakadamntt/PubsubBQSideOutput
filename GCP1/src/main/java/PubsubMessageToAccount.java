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
     public static String regexHelper(String regex, String text) {
        Matcher matcher = Pattern.compile(regex).matcher(text);
        matcher.find();
        return matcher.group(1);
    }

    public static TupleTag<Account> parsedMessages = new TupleTag<Account>(){};
    public static TupleTag<String> unparsedMessages = new TupleTag<String>(){};

    public static PCollectionTuple expand(PCollection<String> input) {
        return input
                .apply("JsonToAccount", ParDo.of(new DoFn<String,Account>() {
                            @ProcessElement
                            public void processElement(ProcessContext context) {
                                String json = context.element();
                                Account a=new Account();
                                Gson gson = new Gson();
                                try {
                                   a.id=Integer.parseInt(regexHelper("\"id\":(\\d+)", json));
                                    a.name= regexHelper("\"name\":\"(.*?)\"", json);
                                    a.surname=regexHelper("\"surname\":\"(.*?)\"", json);
                                    context.output(parsedMessages, a);
                                  
                                } catch (Throwable throwable) {
                                    context.output(unparsedMessages, json);
                                }

                            }
                        })
                        .withOutputTags(parsedMessages, TupleTagList.of(unparsedMessages)));


    }

}

