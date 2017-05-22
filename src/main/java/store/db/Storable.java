package store.db;


import java.util.HashMap;

public interface Storable {

    HashMap<String, String> deconstruct();
    static Storable construct(HashMap<String, String> valuesMap) {
        return null;
    }
}
