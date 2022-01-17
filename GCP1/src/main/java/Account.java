import org.apache.beam.sdk.schemas.JavaFieldSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
@DefaultSchema(JavaFieldSchema.class)
public class Account {
    private int id;
    private  String name;
    private  String surname;
    @Override
    public String toString() {
        return "{" +
                "\nid:" + this.id +
                "\nname:" + this.name +
                "\nsurname:" + this.surname +
                "\n}";
    }
}
