import java.util.logging.LogRecord;

import simpledb.buffer.Buffer;
import simpledb.buffer.BufferAbortException;
import simpledb.buffer.BufferMgr;
import simpledb.file.Block;
import simpledb.server.SimpleDB;
import simpledb.tx.recovery.RecoveryMgr;

public class SimpleDbServerTestBufferScenario1 {
	public static void main(String[] args) {
		SimpleDB.init("simpleDb");
		System.out.println("--------------------SCENARIO 1 START -------------------------");
		
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
	
		System.out.println("Number of availale buffers initially:"+basicBufferMgr.available()+"\n");
		basicBufferMgr.pin(blk1);
		System.out.println("Number of availale buffers:"+basicBufferMgr.available()+"\n");
		Buffer blk2buffer = basicBufferMgr.pin(blk2);
		System.out.println("Number of availale buffers:"+basicBufferMgr.available()+"\n");
		Buffer blk3Buffer = basicBufferMgr.pin(blk3);
		System.out.println("Number of availale buffers:"+basicBufferMgr.available()+"\n");
		basicBufferMgr.pin(blk4);
		System.out.println("Number of availale buffers:"+basicBufferMgr.available()+"\n");
		Buffer blk5buffer = basicBufferMgr.pin(blk5);
		System.out.println("Number of availale buffers:"+basicBufferMgr.available()+"\n");
		basicBufferMgr.pin(blk6);
		System.out.println("Number of availale buffers:"+basicBufferMgr.available()+"\n");
		basicBufferMgr.pin(blk7);
		System.out.println("Number of availale buffers:"+basicBufferMgr.available()+"\n");
		basicBufferMgr.pin(blk8);
		System.out.println("Number of availale buffers:"+basicBufferMgr.available()+"\n");
		basicBufferMgr.pin(blk9);
		System.out.println("--------------------SCENARIO 1 END -------------------------");
		
	}
}
