import org.apache.beam.sdk.schemas.JavaFieldSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaCreate;
@DefaultSchema(JavaFieldSchema.class)
public class Account {
    private  int id;
    private  String name;
    private  String surname;
    
    @SchemaCreate
    public Account(int id,String name,String surname) {
     this.id=id;
     this.name=name;
     this.surname=surname;
    }
}
