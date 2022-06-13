package utils;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;


public class MultiIterator implements Iterator<List<Object>> {
    List<Iterator<Object>> list;
    public MultiIterator(List<Iterator<Object>> l) { list = l; }

    @Override
    public boolean hasNext() {
        list.removeIf( item -> !item.hasNext());
        return list.size() > 0;
    }

    @Override
    public List<Object> next() {
        List<Object> objects = new ArrayList<>();
        list.forEach( item -> objects.add(item.next()));
        return objects;
    }
}
