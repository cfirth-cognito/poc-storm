package Storm.Util;

/**
 * Created by charlie on 31/01/17.
 */
public class Field {

    public int id = 1;
    public String value = "";

    public Field() {
    }

    public Field(int id) {
        this.id = id;
    }

    public Field(String value) {
        this.value = value;
    }

    public Field(int id, String value) {
        this.id = id;
        this.value = value;
    }
}
