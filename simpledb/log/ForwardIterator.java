package simpledb.log;

import java.util.Iterator;

public interface ForwardIterator<K> extends Iterator<K>{

	public K nextForward();
	
}
