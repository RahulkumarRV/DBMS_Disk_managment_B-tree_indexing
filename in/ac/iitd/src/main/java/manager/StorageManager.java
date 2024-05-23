package manager;

import storage.DB;
import storage.File;
import storage.Block;
import Utils.CsvRowConverter;
import index.bplusTree.BPlusTreeIndexFile;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.util.Sources;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.util.Iterator;

import java.nio.ByteBuffer;
import javafx.util.Pair;
import java.util.Arrays;
import java.nio.charset.StandardCharsets;


public class StorageManager {

    private HashMap<String, Integer> file_to_fileid;
    private DB db;
    private BPlusTreeIndexFile indexFile;

    enum ColumnType {
        VARCHAR, INTEGER, BOOLEAN, FLOAT, DOUBLE
    };

    public StorageManager() {
        file_to_fileid = new HashMap<>();
        db = new DB();
    }

    // loads CSV files into DB362
    public void loadFile(String csvFile, List<RelDataType> typeList) {

        // System.out.println("Loading file: " + csvFile);

        String table_name = csvFile;

        if(csvFile.endsWith(".csv")) {
            table_name = table_name.substring(0, table_name.length() - 4);
        }

        // check if file already exists
        assert(file_to_fileid.get(table_name) == null);

        File f = new File();
        try{
            csvFile = getFsPath() + "/" + csvFile;
            BufferedReader br = new BufferedReader(new FileReader(csvFile));
            String line = "";
            int lineNum = 0;

            while ((line = br.readLine()) != null) {

                // csv header line
                if(lineNum == 0){

                    String[] columnNames = CsvRowConverter.parseLine(line);
                    List<String> columnNamesList = new ArrayList<>();
                    for(String columnName : columnNames) {
                        // if columnName contains ":", then take part before ":"
                        String c = columnName;
                        if(c.contains(":")) {
                            c = c.split(":")[0];
                        }
                        columnNamesList.add(c);
                    }

                    Block schemaBlock = createSchemaBlock(columnNamesList, typeList);
                    f.add_block(schemaBlock);
                    lineNum++;
                    continue;
                }

                String[] parsedLine = CsvRowConverter.parseLine(line);
                Object[] row = new Object[parsedLine.length];

                for(int i = 0; i < parsedLine.length; i++) {
                    row[i] = CsvRowConverter.convert(typeList.get(i), parsedLine[i]);
                }

                // convert row to byte array
                byte[] record = convertToByteArray(row, typeList);

                boolean added = f.add_record_to_last_block(record);
                if(!added) {
                    f.add_record_to_new_block(record);
                }
                lineNum++;
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }

        // System.out.println("Done writing file\n");
        int counter = db.addFile(f);
        file_to_fileid.put(table_name, counter);
        return;
    }

    // converts a row to byte array to write to relational file
    private byte[] convertToByteArray(Object[] row, List<RelDataType> typeList) {

        List<Byte> fixed_length_Bytes = new ArrayList<>();
        List<Byte> variable_length_Bytes = new ArrayList<>();
        List<Integer> variable_length = new ArrayList<>();
        List<Boolean> fixed_length_nullBitmap = new ArrayList<>();
        List<Boolean> variable_length_nullBitmap = new ArrayList<>();

        for(int i = 0; i < row.length; i++) {

            if(typeList.get(i).getSqlTypeName().getName().equals("INTEGER")) {
                if(row[i] == null){
                    fixed_length_nullBitmap.add(true);
                    for(int j = 0; j < 4; j++) {
                        fixed_length_Bytes.add((byte) 0);
                    }
                } else {
                    fixed_length_nullBitmap.add(false);
                    int val = (int) row[i];
                    byte[] intBytes = new byte[4];
                    intBytes[0] = (byte) (val & 0xFF);
                    intBytes[1] = (byte) ((val >> 8) & 0xFF);
                    intBytes[2] = (byte) ((val >> 16) & 0xFF);
                    intBytes[3] = (byte) ((val >> 24) & 0xFF);
                    for(int j = 0; j < 4; j++) {
                        fixed_length_Bytes.add(intBytes[j]);
                    }
                }
            } else if(typeList.get(i).getSqlTypeName().getName().equals("VARCHAR")) {
                if(row[i] == null){
                    variable_length_nullBitmap.add(true);
                    for(int j = 0; j < 1; j++) {
                        variable_length_Bytes.add((byte) 0);
                    }
                } else {
                    variable_length_nullBitmap.add(false);
                    String val = (String) row[i];
                    
                    byte[] strBytes = val.getBytes();
                    for(int j = 0; j < strBytes.length; j++) {
                        variable_length_Bytes.add(strBytes[j]);
                    }
                    variable_length.add(strBytes.length);
                }
            } else if (typeList.get(i).getSqlTypeName().getName().equals("BOOLEAN")) {         
                if(row[i] == null){
                    fixed_length_nullBitmap.add(true);
                    fixed_length_Bytes.add((byte) 0);
                } else {
                    fixed_length_nullBitmap.add(false);
                    boolean val = (boolean) row[i];
                    fixed_length_Bytes.add((byte) (val ? 1 : 0));
                }
            } else if (typeList.get(i).getSqlTypeName().getName().equals("FLOAT")) {
                
                if(row[i] == null){
                    fixed_length_nullBitmap.add(true);
                    for(int j = 0; j < 4; j++) {
                        fixed_length_Bytes.add((byte) 0);
                    }
                } else {
                    fixed_length_nullBitmap.add(false);
                    float val = (float) row[i];
                    byte[] floatBytes = new byte[4];
                    int intBits = Float.floatToIntBits(val);
                    floatBytes[0] = (byte) (intBits & 0xFF);
                    floatBytes[1] = (byte) ((intBits >> 8) & 0xFF);
                    floatBytes[2] = (byte) ((intBits >> 16) & 0xFF);
                    floatBytes[3] = (byte) ((intBits >> 24) & 0xFF);
                    for(int j = 0; j < 4; j++) {
                        fixed_length_Bytes.add(floatBytes[j]);
                    }
                }
            } else if (typeList.get(i).getSqlTypeName().getName().equals("DOUBLE")) {
                
                if(row[i] == null){
                    fixed_length_nullBitmap.add(true);
                    for(int j = 0; j < 8; j++) {
                        fixed_length_Bytes.add((byte) 0);
                    }
                } else {
                    fixed_length_nullBitmap.add(false);
                    double val = (double) row[i];
                    byte[] doubleBytes = new byte[8];
                    long longBits = Double.doubleToLongBits(val);
                    doubleBytes[0] = (byte) (longBits & 0xFF);
                    doubleBytes[1] = (byte) ((longBits >> 8) & 0xFF);
                    doubleBytes[2] = (byte) ((longBits >> 16) & 0xFF);
                    doubleBytes[3] = (byte) ((longBits >> 24) & 0xFF);
                    doubleBytes[4] = (byte) ((longBits >> 32) & 0xFF);
                    doubleBytes[5] = (byte) ((longBits >> 40) & 0xFF);
                    doubleBytes[6] = (byte) ((longBits >> 48) & 0xFF);
                    doubleBytes[7] = (byte) ((longBits >> 56) & 0xFF);
                    for(int j = 0; j < 8; j++) {
                        fixed_length_Bytes.add(doubleBytes[j]);
                    }
                }
            } else {
                System.out.println("Unsupported type");
                throw new RuntimeException("Unsupported type");
            }
        }

        short num_bytes_for_bitmap = (short) ((fixed_length_nullBitmap.size() + variable_length_nullBitmap.size() + 7) / 8); // should be in multiples of bytes

        //                       bytes for fixed length and variable length fields          offset & length of var fields
        byte[] result = new byte[fixed_length_Bytes.size() + variable_length_Bytes.size() + 4 * variable_length.size() + num_bytes_for_bitmap];
        int variable_length_offset = 4 * variable_length.size() + 
        
        fixed_length_Bytes.size() + num_bytes_for_bitmap;

        int idx = 0;
        for(; idx < variable_length.size() ; idx ++){
            // first 2 bytes should be offset
            result[idx * 4] = (byte) (variable_length_offset & 0xFF);
            result[idx * 4 + 1] = (byte) ((variable_length_offset >> 8) & 0xFF);

            // next 2 bytes should be length
            result[idx * 4 + 2] = (byte) (variable_length.get(idx) & 0xFF);
            result[idx * 4 + 3] = (byte) ((variable_length.get(idx) >> 8) & 0xFF);

            variable_length_offset += variable_length.get(idx);
        }

        idx = idx * 4;

        // write fixed length fields
        for(int i = 0; i < fixed_length_Bytes.size(); i++, idx++) {
            result[idx] = fixed_length_Bytes.get(i);
        }

        // write null bitmap
        int bitmap_idx = 0;
        for(int i = 0; i < fixed_length_nullBitmap.size(); i++) {
            if(fixed_length_nullBitmap.get(i)) {
                result[idx] |= (1 << (7 - bitmap_idx));
            }
            bitmap_idx++;
            if(bitmap_idx == 8) {
                bitmap_idx = 0;
                idx++;
            }
        }

        for(int i = 0; i < variable_length_nullBitmap.size(); i++) {
            if(variable_length_nullBitmap.get(i)) {
                result[idx] |= (1 << (7 - bitmap_idx));
            }
            bitmap_idx++;
            if(bitmap_idx == 8) {
                bitmap_idx = 0;
                idx++;
            }
        }

        if(bitmap_idx != 0) {
            idx++;
        }

        // write variable length fields
        for(int i = 0; i < variable_length_Bytes.size(); i++, idx++) {
            result[idx] = variable_length_Bytes.get(i);
        }

        return result;
    }

    // helper function for loadFile
    private String getFsPath() throws IOException, ParseException {

        String modelPath = Sources.of(CsvRowConverter.class.getResource("/" + "model.json")).file().getAbsolutePath();
        JSONObject json = (JSONObject) new JSONParser().parse(new FileReader(modelPath));
        JSONArray schemas = (JSONArray) json.get("schemas");

        Iterator itr = schemas.iterator();

        while (itr.hasNext()) {
            JSONObject next = (JSONObject) itr.next();
            if (next.get("name").equals("FILM_DB")) {
                JSONObject operand = (JSONObject) next.get("operand");
                String directory = operand.get("directory").toString();
                return Sources.of(CsvRowConverter.class.getResource("/" + directory)).file().getAbsolutePath();
            }
        }
        return null;
    }

    // write schema block for a relational file
    private Block createSchemaBlock(List<String> columnNames, List<RelDataType> typeList) {

        Block schema = new Block();

        // write number of columns
        byte[] num_columns = new byte[2];
        num_columns[0] = (byte) (columnNames.size() & 0xFF);
        num_columns[1] = (byte) ((columnNames.size() >> 8) & 0xFF);

        schema.write_data(0, num_columns);

        int idx = 0, curr_offset = schema.get_block_capacity();
        for(int i = 0; i < columnNames.size(); i ++){
            // if column type is fixed, then write it
            if(!typeList.get(i).getSqlTypeName().getName().equals("VARCHAR")) {
                
                // write offset
                curr_offset = curr_offset - (columnNames.get(i).length() + 2);
                byte[] offset = new byte[2];
                offset[0] = (byte) (curr_offset & 0xFF);
                offset[1] = (byte) ((curr_offset >> 8) & 0xFF);
                schema.write_data(2 + 2 * idx, offset);
                
                // convert column name to bytes
                byte[] column_name_type = new byte[columnNames.get(i).length() + 2];
                // first byte will tell datatype, 2nd byte will tell length of column name
                // Thus, assert that column name length is less than 256
                assert(columnNames.get(i).length() < 256);

                column_name_type[0] = (byte) (ColumnType.valueOf(typeList.get(i).getSqlTypeName().getName()).ordinal() & 0xFF);
                column_name_type[1] = (byte) (columnNames.get(i).length() & 0xFF);
                for(int j = 0; j < columnNames.get(i).length(); j++) {
                    column_name_type[2 + j] = (byte) columnNames.get(i).charAt(j);
                }

                schema.write_data(curr_offset, column_name_type);
                idx++;
            }
        }

        // write variable length fields
        for(int i = 0; i < columnNames.size(); i++) {
            if(typeList.get(i).getSqlTypeName().getName().equals("VARCHAR")) {
                
                // write offset
                curr_offset = curr_offset - (columnNames.get(i).length() + 2);
                byte[] offset = new byte[2];
                offset[0] = (byte) (curr_offset & 0xFF);
                offset[1] = (byte) ((curr_offset >> 8) & 0xFF); 
                // IMPORTANT: Take care of endianness
                schema.write_data(2 + 2 * idx, offset);
                
                // convert column name to bytes
                byte[] column_name_type = new byte[columnNames.get(i).length() + 2];
                // first byte will tell datatype, 2nd byte will tell length of column name
                // Thus, assert that column name length is less than 256
                assert(columnNames.get(i).length() < 256);

                column_name_type[0] = (byte) (ColumnType.valueOf(typeList.get(i).getSqlTypeName().getName()).ordinal() & 0xFF);
                column_name_type[1] = (byte) (columnNames.get(i).length() & 0xFF);
                for(int j = 0; j < columnNames.get(i).length(); j++) {
                    column_name_type[2 + j] = (byte) columnNames.get(i).charAt(j);
                }

                schema.write_data(curr_offset, column_name_type);
                idx++;
            }
        }

        return schema;
    }

    // should only read one block at a time
    public byte[] get_data_block(String table_name, int block_id){
        int file_id = file_to_fileid.get(table_name);
        return db.get_data(file_id, block_id);
    }

    public boolean check_file_exists(String table_name) {
        return file_to_fileid.get(table_name) != null;
    }

    public boolean check_index_exists(String table_name, String column_name) {
        String index_file_name = table_name + "_" + column_name + "_index";
        return file_to_fileid.get(index_file_name) != null;
    }

    public boolean getBit(byte b, int i) {
        // Shift the bit at the ith position to the rightmost position
        // Then, check if the least significant bit (rightmost bit) is 1 or 0
        return ((b >> i) & 1) == 1;
    }

    public Object[] convertToObjects(byte[] byteArray, List<Integer> typeList, String table_name) {
        List<Object> objects = new ArrayList<>();
        int offset = 0;
        List<Object> variableobjects = new ArrayList<>();
        List<Object> fixedObjects = new ArrayList<>();
        // Iterate over each type in the type list
        int i = 0;
        for(i=0; i<typeList.size(); i++){
            offset = ((byteArray[4 * i + 1] & 0xFF) << 8) | (byteArray[4 * i] & 0xFF);
            int length = ((byteArray[4 * i + 3] & 0xFF) << 8) | (byteArray[4 * i + 2] & 0xFF);
            
            byte[] data = Arrays.copyOfRange(byteArray, offset, offset + length);
            variableobjects.add(new String(data, StandardCharsets.UTF_8));
            if(offset + length == byteArray.length || offset + length == byteArray.length - 1){
                break;
            }
        }

        // get the number of colums from the schema block
        byte[] schemaBlock = get_data_block(table_name, 0);
        int num_of_colums = ((schemaBlock[1] & 0xFF) << 8) | (schemaBlock[0] & 0xFF);
        int num_of_fix_colums = (num_of_colums - i - 1);
        int startOffsetOffix = 4 * i + 4;

        // TODO: CONVER ACCRODING TO CORRECT SIZE OF THE BYTES
        for(int j=0; j<num_of_colums; j++){
            
            if(typeList.get(j) == ColumnType.INTEGER.ordinal() || typeList.get(j) == ColumnType.FLOAT.ordinal()){
                byte[] datai= Arrays.copyOfRange(byteArray, startOffsetOffix, startOffsetOffix + 4);
                int val = ((datai[3] & 0xFF) << 24) |
                    ((datai[2] & 0xFF) << 16) |
                    ((datai[1] & 0xFF) << 8) |
                    (datai[0] & 0xFF);
                fixedObjects.add(val);
                startOffsetOffix += 4;
            }
            else if(typeList.get(j) == ColumnType.BOOLEAN.ordinal()){
                fixedObjects.add(byteArray[startOffsetOffix] != 0);
                startOffsetOffix += 1;
            }
            else if(typeList.get(j) == ColumnType.DOUBLE.ordinal()){
                byte[] datai = Arrays.copyOfRange(byteArray, startOffsetOffix, startOffsetOffix + 8);
                long longBits = 0;
                for (int b = 0; b < 8; b++) {
                    longBits |= ((long) (datai[b] & 0xFF)) << (8 * b);
                }

                double val = Double.longBitsToDouble(longBits);
                fixedObjects.add(val);
                startOffsetOffix += 8;
            }
        }

        // Object[] combinedArray = new Object[fixedObjects.size() + variableobjects.size()];
        ArrayList<Object> combinedArray = new ArrayList<Object>();
        // Copy elements from list1 to the combined array
        int index = 0;
        for (Object obj : fixedObjects) {
            combinedArray.add(obj);
        }

        // Copy elements from list2 to the combined array
        for (Object obj : variableobjects) {
            combinedArray.add(obj);
        }

        int endIndexOfBitMap = (((byteArray[1] & 0xFF) << 8) | (byteArray[0] & 0xFF));
        for(i=0; i<combinedArray.size(); i++) {
            if(getBit(byteArray[startOffsetOffix], i%8)){
                combinedArray.set(i, null);
            }
            if(i != 0 && (i + 1) % 8 == 0){
                startOffsetOffix++;
            }
        }
        return combinedArray.toArray();
    }

    // public List<RelDataType> convertEnumIdsToRelDataType(List<Integer> enumIds) {
    //     List<RelDataType> typeList = new ArrayList<>();
        
    //     for (Integer enumId : enumIds) {
    //         // Map each enum ID to its corresponding RelDataType
    //         switch (enumId) {
    //             case 0:
    //                 typeList.add(RelDataType.VARCHAR); // Assuming RelDataType.VARCHAR represents VARCHAR type
    //                 break;
    //             case 1:
    //                 typeList.add(RelDataType.INTEGER); // Assuming RelDataType.INTEGER represents INTEGER type
    //                 break;
    //             case 2:
    //                 typeList.add(RelDataType.BOOLEAN); // Assuming RelDataType.BOOLEAN represents BOOLEAN type
    //                 break;
    //             case 3:
    //                 typeList.add(RelDataType.FLOAT); // Assuming RelDataType.FLOAT represents FLOAT type
    //                 break;
    //             case 4:
    //                 typeList.add(RelDataType.DOUBLE); // Assuming RelDataType.DOUBLE represents DOUBLE type
    //                 break;
    //             default:
    //                 throw new IllegalArgumentException("Invalid enum ID: " + enumId);
    //         }
    //     }
        
    //     return typeList;
    // }

    public static List<Integer> readEnumtypes(byte[] byteArray) {
        List<Integer> integerValues = new ArrayList<>();

        // Check if byteArray has at least 4 bytes (2 for the number of offsets and 2 for the offset values)
        if (byteArray.length < 4) {
            throw new IllegalArgumentException("Byte array is too short");
        }

        // Read the number of offsets (n)
        int numberOfOffsets = ((byteArray[1] & 0xFF) << 8) | (byteArray[0] & 0xFF);
        
        for (int i = 0; i < numberOfOffsets; i++) {
            int offset = ((byteArray[3 + i * 2] & 0xFF) << 8) | (byteArray[2 + i * 2] & 0xFF);
            if (offset < byteArray.length) {
                int integerValue = byteArray[offset] & 0xFF; // Read the first byte at the specified position
                integerValues.add(integerValue);
            } else {
                // Offset is out of bounds, handle error or ignore
                throw new IllegalArgumentException("offset out of bounds");
            }
        }

        return integerValues;
    }


    public Pair<Integer, Integer> getColumnTypeForColumnValue(byte[] byteArray, String columnValue) {
        // Read the number of column values (n)
        int numberOfColumnValues = ((byteArray[1] & 0xFF) << 8) | (byteArray[0] & 0xFF);
        int indexOfV = 0;
        int indexOfF = 0;
        // Read the offset values
        List<Integer> offsets = new ArrayList<>();
        for (int i = 0; i < numberOfColumnValues; i++) {
            int offset = ((byteArray[3 + (i * 2)] & 0xFF) << 8) | (byteArray[2 + (i * 2)] & 0xFF);
            if (offset + 2 <= byteArray.length) {
                int columnType = byteArray[offset] & 0xFF; // Read the type of the column value
                if(columnType == ColumnType.VARCHAR.ordinal()) indexOfV++;
                else indexOfF++;
                int columnValueLength = byteArray[offset + 1] & 0xFF; // Read the length of the column value
                if (offset + 2 + columnValueLength <= byteArray.length) {
                    String value = new String(byteArray, offset + 2, columnValueLength); // Read the column value
                    if (value.equals(columnValue)) {
                        return new Pair<>(columnType, ( columnType == 0 ? indexOfV : indexOfF));
                    }
                }
            } else {
                // Offset is out of bounds, handle error or ignore
                System.err.println("Offset out of bounds: " + offset);
            }

        }

        // Column value not found
        return null;
    }

    public int getOffsetIndexOfColumnValue(byte[] byteArray, String columnValue) {
        // Read the number of column values (n)
        int numberOfColumnValues = ((byteArray[1] & 0xFF) << 8) | (byteArray[0] & 0xFF);

        // Read the offset values
        for (int i = 0; i < numberOfColumnValues; i++) {
            int offset = ((byteArray[2 + i * 2] & 0xFF) << 8) | (byteArray[3 + i * 2] & 0xFF);
            if (offset + 2 <= byteArray.length) {
                int columnType = byteArray[offset] & 0xFF; // Read the type of the column value
                int columnValueLength = byteArray[offset + 1] & 0xFF; // Read the length of the column value
                if (offset + 2 + columnValueLength <= byteArray.length) {
                    String value = new String(byteArray, offset + 2, columnValueLength); // Read the column value
                    if (value.equals(columnValue)) {
                        return offset;
                    }
                }
            } else {
                // Offset is out of bounds, handle error or ignore
                System.err.println("Offset out of bounds: " + offset);
            }
        }

        // Column value not found
        return -1;
    }

    public byte[] extractColumnBytes(byte[] data, int enumTypeId, int indexOfType, List<Integer> types){
        int zeroCount = 0;
        for (Integer value : types) {
            if (value == 0) {
                zeroCount++;
            }
        }
        if(enumTypeId != ColumnType.VARCHAR.ordinal()){
            // find the first byte of the fixed length section
            int offset = 0;
            int nonZeroCount = types.size() - zeroCount;
            int i = zeroCount * 4;
            // for(i=0; i<types.size() - zeroCount; i++){
            //     offset = ((data[4 * i + 1] & 0xFF) << 8) | (data[4 * i] & 0xFF);
            //     System.out.println("extractcolumnBytes offset = " + offset);
            //     int length = ((data[4 * i + 2] & 0xFF) << 8) | (data[4 * i + 3] & 0xFF);
            //     if(offset + length == data.length || offset + length == data.length - 1){
            //         break;
            //     }
            // }

            int firstByteOfColumn = i; // pointing to the first byte of the fixed length section
            // int firstByteOfColumn = i + (4 * (index - 1));
            for( i=1; i<indexOfType; i++){
                if(types.get(i) == ColumnType.INTEGER.ordinal() || types.get(i) == ColumnType.FLOAT.ordinal()){
                    firstByteOfColumn+=4;
                }
                else if(types.get(i) == ColumnType.BOOLEAN.ordinal()){
                    firstByteOfColumn+=1;
                }
                else if(types.get(i) == ColumnType.DOUBLE.ordinal()){
                    firstByteOfColumn+=8;
                }
            }
            int length = 1;
            if(enumTypeId == 1 || enumTypeId == 3) length = 4;
            else if(enumTypeId == 2) length = 1;
            else if(enumTypeId == 4) length = 8;
            int val = ((data[firstByteOfColumn+3] & 0xFF) << 24) |
                ((data[firstByteOfColumn+2] & 0xFF) << 16) |
                ((data[firstByteOfColumn+1] & 0xFF) << 8) |
                (data[firstByteOfColumn] & 0xFF);
            return Arrays.copyOfRange(data, firstByteOfColumn, firstByteOfColumn + length);

        }
        // if the column is variable length field
        int startIndex = (indexOfType - 1) * 4;
        int offset = ((data[startIndex] & 0xFF) << 8) | (data[startIndex + 1] & 0xFF);
        int length = ((data[startIndex + 2] & 0xFF) << 8) | (data[startIndex + 3] & 0xFF);
        return Arrays.copyOfRange(data, offset, offset + length);
    }
    

    // the order of returned columns should be same as the order in schema
    // i.e., first all fixed length columns, then all variable length columns
    public List<Object[]> get_records_from_block(String table_name, int block_id){
        /* Write your code here */
        // return null if file does not exist, or block_id is invalid
        // return list of records otherwise
        // int file_id = file_to_fileid.get(table_name);
        // System.out.println("get records from block exists");
        byte[] data = get_data_block(table_name, block_id);
        if(file_to_fileid.get(table_name) == null || data == null){
            return null;
        }

        List<Object[]> records = new ArrayList<Object[]>();
        int numRecords = ((data[0] & 0xFF ) << 8) | (data[1] & 0xFF);
        int offsetIndex = 2;
        int previousStartOffset = data.length;
        byte[] schemaBlock = get_data_block(table_name, 0); // schema block
        List<Integer> enumValues = readEnumtypes(schemaBlock);
        
        for(int i=0; i<numRecords; i++){
            int start = ((data[i*2 + 2] & 0xFF ) << 8) | (data[i*2 + 3] & 0xFF);
            int end = previousStartOffset;
            previousStartOffset = start;
            byte[] record = Arrays.copyOfRange(data, start, end);
            records.add(convertToObjects(record, enumValues, table_name));
        }
        return records;
        
    }

    public boolean create_index(String table_name, String column_name, int order) {
        /* Write your code here */
        try{
            byte[] schemaData = get_data_block(table_name, 0); // finding the schema block
            if(file_to_fileid.get(table_name) == null || schemaData == null){
                return false;
            }
            Pair<Integer, Integer> typeAndIndex = getColumnTypeForColumnValue(schemaData, column_name);
            int enumTypeId = typeAndIndex.getKey();
            int indexOfType = typeAndIndex.getValue();
            if(enumTypeId == -1){
                return false;
            }
            
            switch (enumTypeId) {
                case 0:
                    indexFile = new BPlusTreeIndexFile<>(order, String.class);
                    break;
                case 1:
                    indexFile = new BPlusTreeIndexFile<>(order, Integer.class);
                    break;
                case 2:
                    indexFile = new BPlusTreeIndexFile<>(order, Boolean.class);
                    break;
                case 3:
                    indexFile = new BPlusTreeIndexFile<>(order, Float.class);
                    break;
                case 4:
                    indexFile = new BPlusTreeIndexFile<>(order, Double.class);
                    break;
                default:
                    return false; // Handle unsupported data types
            }
            String fileName = table_name + "_" + column_name + "_index";
            /* logic to insert the columns to bplus tree */
            byte[] schemaBlock = get_data_block(table_name, 0); // schema block
            List<Integer> enumValues = readEnumtypes(schemaBlock);

            int block_id = 1;
            byte[] blockOfRecords;
            while((blockOfRecords = get_data_block(table_name, block_id)) != null){
                int numRecords = ((blockOfRecords[0] & 0xFF ) << 8) | (blockOfRecords[1] & 0xFF);
                int offsetIndex = 2;
                int previousStartOffset = blockOfRecords.length;
                for(int i=0; i<numRecords; i++){
                    int start = ((blockOfRecords[(i * 2) + 2] & 0xFF) << 8) | (blockOfRecords[(i * 2) + 3] & 0xFF);
                    int end = previousStartOffset;
                    previousStartOffset = start;
                    byte[] record = Arrays.copyOfRange(blockOfRecords, start, end-1);
                    byte[] data = extractColumnBytes(record, enumTypeId, indexOfType, enumValues);
                    Object value = null;
                    switch (enumTypeId) {
                        case 1:
                            int val = ((data[3] & 0xFF) << 24) |
                                        ((data[2] & 0xFF) << 16) |
                                        ((data[1] & 0xFF) << 8) |
                                        (data[0] & 0xFF);
                            value = val;
                            break;
                        case 0:
                            value = new String(data, StandardCharsets.UTF_8); // Assuming UTF-8 encoding
                            break;
                        case 2:
                            value = data[0] != 0;
                            break;
                        case 3:
                            value = ByteBuffer.wrap(data, 0, data.length).getFloat();
                            break;
                        case 4:
                            value = ByteBuffer.wrap(data, 0, data.length).getDouble();
                            break;
                    }
                    if (value != null) {
                        indexFile.insert(value, block_id); // Insert the value into the index file
                    }
                }
                block_id++;
            }
            int fileIndex = db.addFile(indexFile);
            file_to_fileid.put(fileName, fileIndex);
            return false;
        }catch (Exception e){
            return false;
        }
    }

    // returns the block_id of the leaf node where the key is present
    public int search(String table_name, String column_name, RexLiteral value) {
        /* Write your code here */
        
        try{
            if(!check_file_exists(table_name) || !check_index_exists(table_name, column_name)){
                return -1;
            }

            byte[] data = get_data_block(table_name, 0);
            if(data == null) return -1;

            Pair<Integer, Integer> typeAndIndex = getColumnTypeForColumnValue(data, column_name);
            int enumTypeId = typeAndIndex.getKey();
            int indexOfType = typeAndIndex.getValue();
            if(enumTypeId == -1){
                return -1;
            }
            Object valuei;
            switch (enumTypeId) {
                case 0:
                    valuei = value.toString();
                    break;
                case 1:
                    valuei = Integer.parseInt(value.toString());
                    break;
                case 2:
                    valuei = Boolean.parseBoolean(value.toString());
                    break;
                case 3:
                    valuei = Float.parseFloat(value.toString());
                    break;
                case 4:
                    valuei = Double.parseDouble(value.toString());
                    break;
                default:
                    return -1; // Handle unsupported data types
            }

            return indexFile.search(valuei);
        }
        catch(Exception e){
            return -1;
        }
    }

    public boolean delete(String table_name, String column_name, RexLiteral value) {
        /* Write your code here */
        // Hint: You need to delete from both - the file and the index
        return false;
    }

    // will be used for evaluation - DO NOT modify
    public DB getDb() {
        return db;
    }

    public <T> ArrayList<T> return_bfs_index(String table_name, String column_name) {
        if(check_index_exists(table_name, column_name)) {
            int file_id = file_to_fileid.get(table_name + "_" + column_name + "_index");
            return db.return_bfs_index(file_id);
        } else {
            System.out.println("Index does not exist");
        }
        return null;
    }

}