package file;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.channels.ReadableByteChannel;
import java.security.AccessController;
import java.security.PrivilegedAction;


/**
 * 
1、显然writeWithMappedByteBuffer方式性能最好，且在硬件配置较高情况下优势越加明显
2、在硬件配置较低情况下，writeWithTransferTo比writeWithFileChannel性能稍好
3、在硬件配置较高情况下，writeWithTransferTo和writeWithFileChannel的性能基本持平
4、此外，注意writeWithMappedByteBuffer方式除了占用JVM堆内存外，还要占用额外的native内存（Direct Byte Buffer内存）

内存映射文件使用经验
MappedByteBuffer需要占用“双倍”的内存（对象JVM堆内存和Direct Byte Buffer内存），可以通过-XX:MaxDirectMemorySize参数设置后者最大大小
不要频繁调用MappedByteBuffer的force()方法，因为这个方法会强制OS刷新内存中的数据到磁盘，从而只能获得些微的性能提升（相比IO方式），可以用后面的代码实例进行定时、定量刷新
如果突然断电或者服务器突然Down，内存映射文件数据可能还没有写入磁盘，这时就会丢失一些数据。为了降低这种风险，避免用MappedByteBuffer写超大文件，可以把大文件分割成几个小文件，但不能太小（否则将失去性能优势）
ByteBuffer的rewind()方法将position属性设回为0，因此可以重新读取buffer中的数据；limit属性保持不变，因此可读取的字节数不变
ByteBuffer的flip()方法将一个Buffer由写模式切换到读模式
ByteBuffer的clear()和compact()可以在我们读完ByteBuffer中的数据后重新切回写模式。不同的是clear()会将position设置为0，limit设为capacity，换句话说Buffer被清空了，但Buffer内的数据并没有被清空。如果Buffer中还有未被读取的数据，那调用clear()之后，这些数据会被“遗忘”，再写入就会覆盖这些未读数据。而调用compcat()之后，这些未被读取的数据仍然可以保留，因为它将所有还未被读取的数据拷贝到Buffer的左端，然后设置position为紧随未读数据之后，limit被设置为capacity，未读数据不会被覆盖
 */
/**
 * NIO写大文件比较
 * @author Will
 * 
 */
public class WriteBigFileComparison {

	// data chunk be written per time
	private static final int DATA_CHUNK = 128 * 1024 * 1024; 

	// total data size is 2G
	private static final long LEN = 2L * 1024 * 1024 * 1024L; 

	
	public static void writeWithFileChannel() throws IOException {
		File file = new File("e:/test/fc.dat");
		if (file.exists()) {
			file.delete();
		}

		RandomAccessFile raf = new RandomAccessFile(file, "rw");
		FileChannel fileChannel = raf.getChannel();

		byte[] data = null;
		long len = LEN;
		ByteBuffer buf = ByteBuffer.allocate(DATA_CHUNK);
		int dataChunk = DATA_CHUNK / (1024 * 1024);
		while (len >= DATA_CHUNK) {
			System.out.println("write a data chunk: " + dataChunk + "MB");

			buf.clear(); // clear for re-write
			data = new byte[DATA_CHUNK];
			for (int i = 0; i < DATA_CHUNK; i++) {
				buf.put(data[i]);
			}

			data = null;

			buf.flip(); // switches a Buffer from writing mode to reading mode
			fileChannel.write(buf);
			fileChannel.force(true);

			len -= DATA_CHUNK;
		}

		if (len > 0) {
			System.out.println("write rest data chunk: " + len + "B");
			buf = ByteBuffer.allocateDirect((int) len);
			data = new byte[(int) len];
			for (int i = 0; i < len; i++) {
				buf.put(data[i]);
			}

			buf.flip(); // switches a Buffer from writing mode to reading mode, position to 0, limit not changed
			fileChannel.write(buf);
			fileChannel.force(true);
			data = null;
		}

		fileChannel.close();
		raf.close();
	}

	/**
	 * write big file with MappedByteBuffer
	 * @throws IOException
	 */
	public static void writeWithMappedByteBuffer() throws IOException {
		File file = new File("e:/test/mb.dat");
		if (file.exists()) {
			file.delete();
		}

		RandomAccessFile raf = new RandomAccessFile(file, "rw");
		FileChannel fileChannel = raf.getChannel();
		int pos = 0;
		MappedByteBuffer mbb = null;
		byte[] data = null;
		long len = LEN;
		int dataChunk = DATA_CHUNK / (1024 * 1024);
		while (len >= DATA_CHUNK) {
			System.out.println("write a data chunk: " + dataChunk + "MB");

			mbb = fileChannel.map(MapMode.READ_WRITE, pos, DATA_CHUNK);
			data = new byte[DATA_CHUNK];
			mbb.put(data);

			data = null;

			len -= DATA_CHUNK;
			pos += DATA_CHUNK;
		}

		if (len > 0) {
			System.out.println("write rest data chunk: " + len + "B");

			mbb = fileChannel.map(MapMode.READ_WRITE, pos, len);
			data = new byte[(int) len];
			mbb.put(data);
		}

		data = null;
		unmap(mbb);   // release MappedByteBuffer
		fileChannel.close();
	}
	
	public static void writeWithTransferTo() throws IOException {
		File file = new File("e:/test/transfer.dat");
		if (file.exists()) {
			file.delete();
		}
		
		RandomAccessFile raf = new RandomAccessFile(file, "rw");
		FileChannel toFileChannel = raf.getChannel();
		
		long len = LEN;
		byte[] data = null;
		ByteArrayInputStream bais = null;
		ReadableByteChannel fromByteChannel = null;
		long position = 0;
		int dataChunk = DATA_CHUNK / (1024 * 1024);
		while (len >= DATA_CHUNK) {
			System.out.println("write a data chunk: " + dataChunk + "MB");
			
			data = new byte[DATA_CHUNK];
			bais = new ByteArrayInputStream(data);
			fromByteChannel = Channels.newChannel(bais);
			
			long count = DATA_CHUNK;
			toFileChannel.transferFrom(fromByteChannel, position, count);
			
			data = null;
			position += DATA_CHUNK;
			len -= DATA_CHUNK;
		}
		
		if (len > 0) {
			System.out.println("write rest data chunk: " + len + "B");

			data = new byte[(int) len];
			bais = new ByteArrayInputStream(data);
			fromByteChannel = Channels.newChannel(bais);
			
			long count = len;
			toFileChannel.transferFrom(fromByteChannel, position, count);
		}
		
		data = null;
		toFileChannel.close();
		fromByteChannel.close();
	}
	
	/**
	 * 在MappedByteBuffer释放后再对它进行读操作的话就会引发jvm crash，在并发情况下很容易发生
	 * 正在释放时另一个线程正开始读取，于是crash就发生了。所以为了系统稳定性释放前一般需要检
	 * 查是否还有线程在读或写
	 * @param mappedByteBuffer
	 */
	public static void unmap(final MappedByteBuffer mappedByteBuffer) {
		try {
			if (mappedByteBuffer == null) {
				return;
			}
			
			mappedByteBuffer.force();
			AccessController.doPrivileged(new PrivilegedAction<Object>() {
				@Override
				@SuppressWarnings("restriction")
				public Object run() {
					try {
						Method getCleanerMethod = mappedByteBuffer.getClass()
								.getMethod("cleaner", new Class[0]);
						getCleanerMethod.setAccessible(true);
						//sun.misc.Cleaner cleaner = (sun.misc.Cleaner) getCleanerMethod.invoke(mappedByteBuffer, new Object[0]);
						//cleaner.clean();
						
					} catch (Exception e) {
						e.printStackTrace();
					}
					System.out.println("clean MappedByteBuffer completed");
					return null;
				}
			});

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) throws IOException {
		//StopWatch sw = new StopWatch();
		
		//.startWithTaskName("write with file channel's write(ByteBuffer)");
		writeWithFileChannel();
		//.stopAndPrint();
		
		//sw.startWithTaskName("write with file channel's transferTo");
		writeWithTransferTo();
		//sw.stopAndPrint();
		
		//sw.startWithTaskName("write with MappedByteBuffer");
		writeWithMappedByteBuffer();
		//sw.stopAndPrint();
	}

}