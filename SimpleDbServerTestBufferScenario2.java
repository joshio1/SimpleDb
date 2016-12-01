import java.util.logging.LogRecord;

import simpledb.buffer.Buffer;
import simpledb.buffer.BufferAbortException;
import simpledb.buffer.BufferMgr;
import simpledb.file.Block;
import simpledb.server.SimpleDB;
import simpledb.tx.recovery.RecoveryMgr;

public class SimpleDbServerTestBufferScenario2 {
	public static void main(String[] args) {
		SimpleDB.init("simpleDb");
		System.out.println("--------------------SCENARIO 2 START -------------------------");
		Block blk1 = new Block("filename",1);
		Block blk2 = new Block("filename",2);
		Block blk3 = new Block("filename",3);
		Block blk4 = new Block("filename",4);
		Block blk5 = new Block("filename",5);
		Block blk6 = new Block("filename",6);
		Block blk7 = new Block("filename",7);
		Block blk8 = new Block("filename",8);
		Block blk9 = new Block("filename",9);
		Block blk10 = new Block("filename",10);
		
		BufferMgr basicBufferMgr = new SimpleDB().bufferMgr();
		
		basicBufferMgr.pin(blk1);
		
		Buffer blk2buffer = basicBufferMgr.pin(blk2);
		
		Buffer blk3Buffer = basicBufferMgr.pin(blk3);
		
		basicBufferMgr.pin(blk4);
		
		basicBufferMgr.pin(blk5);
		
		basicBufferMgr.pin(blk6);
		
		basicBufferMgr.pin(blk7);
		
		basicBufferMgr.pin(blk8);
		
		basicBufferMgr.unpin(blk3Buffer);
		
		basicBufferMgr.unpin(blk2buffer);
		
		Buffer blk9buffer = basicBufferMgr.pin(blk9);
		
		basicBufferMgr.unpin(blk9buffer);
		
		basicBufferMgr.pin(blk10);
		
		System.out.println("--------------------SCENARIO 2 END -------------------------");
		
	}
}
