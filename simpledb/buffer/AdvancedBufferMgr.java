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
//      bufferpool = new Buffer[numbuffs];
	   	this.maxSize = numbuffs;
	   	numAvailable = numbuffs;
//      for (int i=0; i<numbuffs; i++)
//         bufferpool[i] = new Buffer();
	   	bufferList = new FixedSizeQueue<>(numbuffs);
	    bufferPoolMap = new HashMap<Block, Buffer>();
   }
   
   /**
    * Flushes the dirty buffers modified by the specified transaction.
    * @param txnum the transaction's id number
    */
   synchronized void flushAll(int txnum) {
//      for (Buffer buff : bufferpool)
//         if (buff.isModifiedBy(txnum)){
//        	 buff.flush();
//         }
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
//	  System.out.println("Blk "+blk);
      Buffer buff = findExistingBuffer(blk);
      if (buff == null) {
         buff = chooseUnpinnedBuffer();
         if (buff == null)
            return null;
         //Add block - buffer mapping
         buff.assignToBlock(blk);
         updateBufferPoolMap(buff);
      }
      if (!buff.isPinned())
         numAvailable--;
      buff.pin();
//      System.out.println("Map "+bufferPoolMap);
//      System.out.println("List "+bufferList);
      return buff;
   }
   
   /**
    * This method updates the bufferpoolmap.
    * If a block is already stored in a buffer and we need to store a new block in that buffer,
    * we need to delete the entry for that buffer in the bufferpoolmap
    * @param buff
    */
	void updateBufferPoolMap(Buffer buff) {
		// If buffer is already allocated, remove the entry
		for (Map.Entry<Block, Buffer> entry : bufferPoolMap.entrySet()) {
			if (entry.getValue().equals(buff)) {
				bufferPoolMap.remove(entry.getKey());
				break;
			}
		}
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
      return buff;
   }
   
   /**
    * Unpins the specified buffer.
    * @param buff the buffer to be unpinned
    */
   synchronized void unpin(Buffer buff) {
      buff.unpin();
      if (!buff.isPinned())
         numAvailable++;
//      System.out.println("Unpin"+buff);
//      System.out.println(bufferPoolMap);
   }
   
   /**
    * Returns the number of available (i.e. unpinned) buffers.
    * @return the number of available buffers
    */
   int available() {
      return numAvailable;
   }
   
   private Buffer findExistingBuffer(Block blk) {
//	   Fetch using bufferpoolmap
	   Buffer buffer = bufferPoolMap.get(blk);
	   if(buffer!=null){
//		   System.out.println("hit");
		   return buffer;
	   }
	   else{
		   return null;
	   }
   }
   
   private Buffer chooseUnpinnedBuffer() {
//      for (Buffer buff : bufferpool)
//         if (!buff.isPinned())
//         return buff;
//      return null;
	   //First fill the buffer
	   if(bufferList.size()<maxSize){
//		   System.out.println("Here");
		   Buffer buffer = new Buffer();
		   bufferList.add(buffer);
		   return buffer;
	   }
	   else{
		   for (Buffer buff : bufferList)
			   if (buff!=null && !buff.isPinned()){
				   bufferList.remove(buff);
				   bufferList.add(buff);
				   return buff;
			   }
	   }
      return null;
   }
}