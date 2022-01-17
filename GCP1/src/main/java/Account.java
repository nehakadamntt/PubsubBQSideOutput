import org.apache.beam.sdk.schemas.JavaFieldSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
@DefaultSchema(JavaFieldSchema.class)
public class Account {
    @javax.annotation.Nullable int id;
    @javax.annotation.Nullable String name;
    @javax.annotation.Nullable String surname;
    @Override
    public String toString() {
        return "{" +
                "\nid:" + this.id +
                "\nname:" + this.name +
                "\nsurname:" + this.surname +
                "\n}";
    }
}
