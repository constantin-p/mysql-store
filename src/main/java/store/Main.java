package store;

import store.util.Config;

public class Main {

    public static void main(String[] args) {
        Config.getInstance().loadConfig("store", "store.properties");
        System.out.println(Config.getInstance().getConfig("store"));
    }
}
