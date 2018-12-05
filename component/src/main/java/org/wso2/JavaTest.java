package org.wso2;

import java.util.HashMap;

public class JavaTest {

    public static void main(String[] args) {
        HashMap map = new HashMap();
        map.put(new fun("one"), "one");
        map.put(new fun("two"), "two");
        System.out.println(((fun)map.get(new fun("one"))).store);
    }

    public static class fun {
        String store;
        fun(String abc) {
            this.store = abc;
        }
    }

}
