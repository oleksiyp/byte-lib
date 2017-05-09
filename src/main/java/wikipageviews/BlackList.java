package wikipageviews;

import byte_lib.io.ByteFiles;
import byte_lib.string.ByteString;

import java.io.File;
import java.util.HashSet;
import java.util.Set;

public class BlackList {
    private Set<ByteString> list;

    public BlackList(File blacklist) {
        list = new HashSet<>();
        ByteFiles.loadCollection(blacklist, list);
    }


    public boolean isForbidden(ByteString resource) {
        return list.contains(resource);
    }
}
