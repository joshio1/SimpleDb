package simpledb.buffer;

import java.util.HashMap;
import java.util.Map;

import simpledb.file.Block;
import simpledb.file.FileMgr;

/**
 * Manages the pinning and unpinning of buffers to blocks.
 * @author Edward Sciore
 *
 */
class AdvancedBufferMgr {
//   private Buffer[] bufferpool;
	private FixedSizeQueue<Buffer> bufferList;
	private int numAvailable;
	private int maxSize;
	private Map<Block,Buffer> bufferPoolMap;
   
   /**
    * Creates a buffer manager having the specified number 
    * of buffer slots.
    * This constructor depends on both the {@link FileMgr} and
    * {@link simpledb.log.LogMgr LogMgr} objects 
    * that it gets from the class
    * {@link simpledb.server.SimpleDB}.
    * Those objects are created during system initialization.
    * Thus this constructor cannot be called until 
    * {@link simpledb.server.SimpleDB#initFileAndLogMgr(String)} or
    * is called first.
    * @param numbuffs the number of buffer slots to allocate
    */
   AdvancedBufferMgr(int numbuffs) {
	   	maxSize = numbuffs;
	   	numAvailable = numbuffs;
	   	bufferList = new FixedSizeQueue<>(numbuffs);
	    bufferPoolMap = new HashMap<Block, Buffer>();
   }
   
   /**
    * Flushes the dirty buffers modified by the specified transaction.
    * @param txnum the transaction's id number
    */
   synchronized void flushAll(int txnum) {
	   for(Map.Entry<Block, Buffer> entry : bufferPoolMap.entrySet()){
		   if(entry.getValue().isModifiedBy(txnum)){
			   entry.getValue().flush();
			   bufferPoolMap.remove(entry);
		   }
	   }
   }
   
   /**
    * Pins a buffer to the specified block. 
    * If there is already a buffer assigned to that block
    * then that buffer is used;
    * otherwise, an unpinned buffer from the pool is chosen.
    * Returns a null value if there are no available buffers.
    * @param blk a reference to a disk block
    * @return the pinned buffer
    */
   synchronized Buffer pin(Block blk) {
      Buffer buff = findExistingBuffer(blk);
      if (buff == null) {
         buff = chooseUnpinnedBuffer();
         if (buff == null)
            return null;
         buff.assignToBlock(blk);
         //Update block - buffer mapping
         updateBufferPoolMap(buff);
      }
      if (!buff.isPinned())
         numAvailable--;
      buff.pin();
      System.out.println("Pin count of buffer "+bufferList.indexOf(buff)+" is "+buff.getPins());
      
      System.out.println("BufferPool Queue : "+bufferList);
      return buff;
   }
   
   /**
    * This method updates the bufferpoolmap.
    * If a block is already stored in a buffer and we need to store a new block in that buffer,
    * we need to delete the entry for that buffer in the bufferpoolmap
    * @param buff
    */
	void updateBufferPoolMap(Buffer buff) {
		// If buffer is already allocated to some other block, remove the entry from the map.
		for (Map.Entry<Block, Buffer> entry : bufferPoolMap.entrySet()) {
			if (entry.getValue().equals(buff)) {
				bufferPoolMap.remove(entry.getKey());
				break;
			}
		}
		//Update the bufferpoolmap with the new block - buffer assignment
		bufferPoolMap.put(buff.block(), buff);
	}
   
   /**
    * Allocates a new block in the specified file, and
    * pins a buffer to it. 
    * Returns null (without allocating the block) if 
    * there are no available buffers.
    * @param filename the name of the file
    * @param fmtr a pageformatter object, used to format the new block
    * @return the pinned buffer
    */
   synchronized Buffer pinNew(String filename, PageFormatter fmtr) {
      Buffer buff = chooseUnpinnedBuffer();
      if (buff == null)
         return null;
      buff.assignToNew(filename, fmtr);
      updateBufferPoolMap(buff);
      numAvailable--;
      buff.pin();
      System.out.println("Pin count of buffer "+bufferList.indexOf(buff)+" is "+buff.getPins());
      return buff;
   }
   
   /**
    * Unpins the specified buffer.
    * @param buff the buffer to be unpinned
    */
   synchronized void unpin(Buffer buff) {
      buff.unpin();
     // System.out.println("Buffer unpinned:"+bufferList.indexOf(buff));
	  if (!buff.isPinned())
         numAvailable++;
   }
   
   /**
    * Returns the number of available (i.e. unpinned) buffers.
    * @return the number of available buffers
    */
   int available() {
      return numAvailable;
   }
   
   /**
    * This method will return the buffer for the block if it exists and null,otherwise.
    * @param blk
    * @return
    */
   private Buffer findExistingBuffer(Block blk) {
	   return bufferPoolMap.get(blk);
   }
   
   private Buffer chooseUnpinnedBuffer() {
	   //First fill the buffer
	   if(bufferList.size()<maxSize){
		   //BufferPool still has space, first fill the BufferPool
		   Buffer buffer = new Buffer();
		   bufferList.addLast(buffer); //Add the buffer to the last position in the queue
		   System.out.println("Buffer"+ (bufferList.indexOf(buffer)+1)+" is mapped next");
		   return buffer;
	   }
	   else{
		   //BufferPool is not empty
		   for (Buffer buff : bufferList)
			   if (buff!=null && !buff.isPinned()){ //Choose the first unpinned buffer from the Queue
				   System.out.println("Buffer mapped to the block"+buff+" is replaced next");
				   bufferList.addLast(buff); //Add the buffer to the last position in the queue even if it is already present in the queue
				   return buff;
			   }
	   }
	  return null;
   }
}