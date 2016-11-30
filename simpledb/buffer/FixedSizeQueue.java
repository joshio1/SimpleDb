package simpledb.buffer;

import java.util.ArrayList;

public class FixedSizeQueue<K> extends ArrayList<K>{

	private int maxSize;
	
	public FixedSizeQueue(int maxSize) {
		this.maxSize = maxSize;
	}
	
	@Override
	public boolean add(K e) {
		if (this.size() + 1 > maxSize) {
			return false;
		} else {
			return super.add(e);
		}
	}
	
	/**
	 * This element will push the element to the last position in the queue.
	 * (Even if the element is present in the queue)
	 * @param e
	 * @return
	 */
	public boolean addLast(K e)
	{
		this.remove(e);
		return super.add(e);
	}
	
	public K getFirst(){
		return get(0);
	}
	
	public K getLast(){
		return get(size()-1);
	}
	
}
