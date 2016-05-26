import java.io.File;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;


public class AvroJava1 {
	public static void main(String[] args) throws IOException{
		Schema schema=new Schema.Parser().parse("item.avsc");
		GenericRecord item1=new GenericData.Record(schema);
		item1.put("itemname", "Laptops");
		item1.put("itemid", 1);
		item1.put("qty", 1);
		item1.put("price", 30000);
		
		GenericRecord item2=new GenericData.Record(schema);
		item2.put("itemname", "FlashDrive");
		item2.put("itemid", 2);
		item2.put("qty", 2);
		item2.put("price", 1000);
		DatumWriter<GenericRecord> datumWriter=new GenericDatumWriter<GenericRecord>(schema);
		DataFileWriter<GenericRecord> dataFileWriter=new DataFileWriter<GenericRecord>(datumWriter);
		dataFileWriter.create(schema, new File("myavro.txt"));
		dataFileWriter.append(item1);
		dataFileWriter.append(item2);
		System.out.println("Data has been inserted successfully");
	}
}
