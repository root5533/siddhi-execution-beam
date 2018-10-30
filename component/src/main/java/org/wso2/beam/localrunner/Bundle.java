package org.wso2.beam.localrunner;

import org.apache.beam.sdk.values.PCollection;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class Bundle {

    private static Bundle source;
    private HashMap<PCollection, SourceWrapper> readers = new HashMap<PCollection, SourceWrapper>();

    public static Bundle create() {
        if (source == null) {
            source = new Bundle();
        }
        return source;
    }

    private Bundle() {}

    public void setSourceReader(SourceWrapper reader, Map pCollections) {
        for (Iterator iter = pCollections.values().iterator(); iter.hasNext();) {
            this.readers.put((PCollection) iter.next(), reader);
        }
    }

    public HashMap<PCollection, SourceWrapper> getReaders() {
        return this.readers;
    }

    public SourceWrapper getReader(PCollection key) {
        return readers.get(key);
    }

}
