package file;
import java.io.File;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;

public class file {
	/**
	 * 保存文件
	 * @param path
	 * @param info
	 */
	public static void writeWithMappedByteBuffer(String path, String info) {
		try {
			File file = new File(path);
			if (file.exists()) {
				file.delete();
			}
			RandomAccessFile raf = new RandomAccessFile(file, "rw");
			FileChannel fileChannel = raf.getChannel();
			MappedByteBuffer mbb = null;
			byte[] data = info.getBytes();
			mbb = fileChannel.map(MapMode.READ_WRITE, 0, data.length);
			mbb.put(data);
			data = null;
			mbb=null;
			//unmap(mbb);   // release MappedByteBuffer
			fileChannel.close();
		} catch (Exception e) {
			e.printStackTrace();
		} 
	}
	
	/*public static void main(String[] args){
		String path = "d:/a/b.json";
		String head="{\"index\":{\"_index\":\"myindex\",\"_type\":\"ryxxb\"}}";
		String row = "{\"ID \":\"6\",\"USER_CODE\":\"cd0980\",\"USER_NAME\":\"李三三\",\"ZJHM\":\"ABC08328322233\",\"HEG\":25}";
		StringBuffer sb = new StringBuffer();
		for(int i=0;i<50000;i++){
			sb.append(head);
			sb.append("\n");
			sb.append(row);
			sb.append("\n");
		}
		writeWithMappedByteBuffer(path ,sb.toString());
	
	}*/
}
