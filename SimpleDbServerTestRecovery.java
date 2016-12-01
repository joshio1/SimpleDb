import simpledb.buffer.Buffer;
import simpledb.buffer.BufferMgr;
import simpledb.file.Block;
import simpledb.server.SimpleDB;
import simpledb.tx.recovery.LogRecordIterator;
import simpledb.tx.recovery.RecoveryMgr;

public class SimpleDbServerTestRecovery {
	public static void main(String[] args) {
		SimpleDB.init("simpleDb");
//		SimpleDB.LOG_FILE
//		RecoveryMgr rm = new RecoveryMgr(123);
//		Block blk1 = new Block("filename",1);
//		Block blk2 = new Block("filename",2);
//		Block blk3 = new Block("filename",3);
//		Block blk4 = new Block("filename",4);
//		Block blk5 = new Block("filename",5);
//		Block blk6 = new Block("filename",6);
//		Block blk7 = new Block("filename",7);
//		Block blk8 = new Block("filename",8);
//		Block blk9 = new Block("filename",9);
//		Block blk10 = new Block("filename",10);
//		BufferMgr basicBufferMgr = new SimpleDB().bufferMgr();
//		basicBufferMgr.pin(blk1);
//		Buffer blk2buffer = basicBufferMgr.pin(blk2);
//		basicBufferMgr.pin(blk3);
//		basicBufferMgr.pin(blk4);
//		Buffer blk5buffer = basicBufferMgr.pin(blk5);
//		basicBufferMgr.pin(blk6);
//		basicBufferMgr.pin(blk7);
//		basicBufferMgr.pin(blk8);
//		int lsn = rm.setInt(blk2buffer, 4, 100);
//		blk2buffer.setInt(4, 100, 123, lsn);
//		
//		rm.commit();
//		
//		LogRecordIterator it = new LogRecordIterator();
//		System.out.println("Backward");
////		while(it.hasNext())
//		System.out.println(it.next());
//		System.out.println(it.next());
//		System.out.println(it.next());
////		System.out.println("Forward");
////		while(it.hasNextForward())
//		System.out.println(it.nextForward());
//		System.out.println(it.nextForward());
//		System.out.println(it.nextForward());
	
		BufferMgr basicBufferMgr = new SimpleDB().bufferMgr();
		{
		//Txn12
		RecoveryMgr rm = new RecoveryMgr(12);
		Block blk0 = new Block("filename",9);
		Buffer blk0buffer = basicBufferMgr.pin(blk0);
		int lsn = rm.setInt(blk0buffer, 4, 2);
		blk0buffer.setInt(4, 2, 12, lsn);
		basicBufferMgr.unpin(blk0buffer);
		basicBufferMgr.pin(blk0);
		rm.commit();
		basicBufferMgr.flushAll(12);
		}
		
		{
		//Txn23
		RecoveryMgr rm1 = new RecoveryMgr(23);
		Block blk1 = new Block("filename",4);
		Buffer blk1buffer = basicBufferMgr.pin(blk1);
		int lsn1 = rm1.setString(blk1buffer, 4, "W");
		blk1buffer.setString(4, "W", 23, lsn1);
		rm1.recover();
		}
		
		System.out.println("Done");
	}
}
