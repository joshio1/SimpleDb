package simpledb.log;

import static simpledb.file.Page.INT_SIZE;
import static simpledb.file.Page.BLOCK_SIZE;
import simpledb.file.*;
import java.util.Iterator;

/**
 * A class that provides the ability to move through the
 * records of the log file in reverse order.
 * 
 * @author Edward Sciore
 */
class LogIterator implements ForwardIterator<BasicLogRecord> {
   private Block blk;
   private Page pg = new Page();
   private int currentrec;
   
   /**
    * Creates an iterator for the records in the log file,
    * positioned after the last log record.
    * This constructor is called exclusively by
    * {@link LogMgr#iterator()}.
    */
   LogIterator(Block blk) {
      this.blk = blk;
      pg.read(blk);
      currentrec = pg.getInt(LogMgr.LAST_POS);
   }
   
   /**
    * Determines if the current log record
    * is the earliest record in the log file.
    * @return true if there is an earlier record
    */
   public boolean hasNext() {
      return currentrec>0 || blk.number()>0;
   }
   
   /**
    * Determines if the current log record
    * is the earliest record in the log file.
    * @return true if there is an earlier record
    */
   public boolean hasNextForward() {
	   if(blk.number()<LogMgr.currentblk.number())
		   return true;
	   else if((blk.number()==LogMgr.currentblk.number())&&(currentrec+INT_SIZE<LogMgr.currentpos))
			return true;
			else
				return false;
   }
   
   /**
    * Moves to the next log record in reverse order.
    * If the current log record is the earliest in its block,
    * then the method moves to the next oldest block,
    * and returns the log record from there.
    * @return the next earliest log record
    */
   public BasicLogRecord next() {
      if (currentrec == 0) 
         moveToNextBlock();
      currentrec = pg.getInt(currentrec);
      return new BasicLogRecord(pg, currentrec+INT_SIZE+INT_SIZE);
   }
   
   /**
    * Moves to the next log record in forward order.
    * If the current log record is the earliest in its block,
    * then the method moves to the next oldest block,
    * and returns the log record from there.
    * @return the next earliest log record
    */
   public BasicLogRecord nextForward() {
	 //Actual Log Record is at 8 bytes after the current rec.
	 //First 4 bytes contains the last record position for the previous record.
	 //Second 4 bytes contains the size of the record.
	   int recsize=0;
      if (currentrec + INT_SIZE + INT_SIZE >= Page.BLOCK_SIZE) {
    	  moveToNextForwardBlock();
    	  recsize = pg.getInt(currentrec+INT_SIZE);    	  
      }
      else{
    	  recsize = pg.getInt(currentrec+INT_SIZE);
    	  if(recsize<=0){
    		  moveToNextForwardBlock();
    		  recsize = pg.getInt(currentrec+INT_SIZE);
    	  }
      }
      BasicLogRecord lr = new BasicLogRecord(pg, currentrec+INT_SIZE+INT_SIZE);
      currentrec += recsize;
      return lr;
   }
   
   public void remove() {
      throw new UnsupportedOperationException();
   }
   
   /**
    * Moves to the next log block in reverse order,
    * and positions it after the last record in that block.
    */
   private void moveToNextBlock() {
      blk = new Block(blk.fileName(), blk.number()-1);
      pg.read(blk);
      currentrec = pg.getInt(LogMgr.LAST_POS);
   }
   
   /**
    * Moves to the next log block in forward order,
    * and positions it after the last record in that block.
    */
   private void moveToNextForwardBlock() {
      blk = new Block(blk.fileName(), blk.number()+1);
      pg.read(blk);
      currentrec = 0;
   }
}