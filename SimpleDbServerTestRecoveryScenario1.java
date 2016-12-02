import simpledb.buffer.Buffer;
import simpledb.buffer.BufferMgr;
import simpledb.file.Block;
import simpledb.server.SimpleDB;
import simpledb.tx.recovery.LogRecordIterator;
import simpledb.tx.recovery.RecoveryMgr;

public class SimpleDbServerTestRecoveryScenario1 {
	public static void main(String[] args) {
		SimpleDB.init("simpleDb");

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
		rm1.commit();
		}
		
		LogRecordIterator it = new LogRecordIterator();
		System.out.println("Backward Iteration");
		while(it.hasNext())
			System.out.println(it.next());
		System.out.println("Forward");
		while(it.hasNextForward())
			System.out.println(it.nextForward());
		System.out.println("Done");
	}
}
