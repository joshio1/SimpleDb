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
   private int currentRecForward;
   
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
      currentRecForward = INT_SIZE; //Since at position 0, currentrec is stored for that block.
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
      return currentrec<BLOCK_SIZE || blk.number()>0;
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
      if (currentRecForward >= Page.BLOCK_SIZE) 
         moveToNextForwardBlock();
//      currentrec = pg.getInt(currentrec);
      BasicLogRecord lr = new BasicLogRecord(pg, currentRecForward+INT_SIZE);
      int recsize = pg.getInt(currentRecForward);
      pg.getInt(currentRecForward+INT_SIZE);
      currentRecForward += recsize;
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
      currentrec = pg.getInt(LogMgr.LAST_POS);
   }
}