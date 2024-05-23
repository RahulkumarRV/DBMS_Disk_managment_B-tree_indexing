package optimizer.rel;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexCall;
import manager.StorageManager;

import java.util.ArrayList;
import java.util.List;

// Operator trigged when doing indexed scan
// Matches SFW queries with indexed columns in the WHERE clause
public class PIndexScan extends TableScan implements PRel {
    
        private final List<RexNode> projects;
        private final RelDataType rowType;
        private final RelOptTable table;
        private final RexNode filter;
    
        public PIndexScan(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table, RexNode filter, List<RexNode> projects) {
            super(cluster, traitSet, table);
            this.table = table;
            this.rowType = deriveRowType();
            this.filter = filter;
            this.projects = projects;
        }
    
        @Override
        public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
            return new PIndexScan(getCluster(), traitSet, table, filter, projects);
        }
    
        @Override
        public RelOptTable getTable() {
            return table;
        }

        @Override
        public String toString() {
            return "PIndexScan";
        }

        public String getTableName() {
            return table.getQualifiedName().get(1);
        }

        public int compare(Object key1, Object key2) {
            if (key1 instanceof Comparable && key2 instanceof Comparable) {
                Comparable<Object> comparableKey1 = (Comparable<Object>) key1;
                try {
                    // Perform the comparison
                    return comparableKey1.compareTo(key2);
                } catch (ClassCastException e) {
                    // Handle the case where the objects are not compatible for comparison
                    return key1.toString().compareTo(key2.toString());
                }
            }
            // Default comparison result if Comparable interface is not implemented
            return key1.toString().compareTo(key2.toString());
        }

        public boolean evaluateCondition(String operator, Object operand1, Object operand2) {
            int comparisonResult = compare(operand1, operand2);
            switch (operator) {
                case ">":
                    return comparisonResult > 0;
                case "<":
                    return comparisonResult < 0;
                case ">=":
                    return comparisonResult >= 0;
                case "<=":
                    return comparisonResult <= 0;
                case "=":
                    return comparisonResult == 0;
                case "<>":
                    return comparisonResult != 0;
                default:
                    return false;
            }
        
        }

        public Object createFieldValue(RelDataType fieldType, String value) {
            
            try {
                String type = fieldType.toString();
                
                // Convert the string value to the appropriate type
                if (type.equals("INTEGER")) {
                    return Integer.parseInt(value);
                } 
                else if (type.equals("FLOAT")) {
                    return Float.parseFloat(value);
                } 
                else if (type.equals("BOOLEAN")) {
                    return Boolean.parseBoolean(value);
                } 
                else if (type.equals("DOUBLE")) {
                    return Double.parseDouble(value);
                } else if (type.equals("VARCHAR")) {
                    return value;
                } else {
                    // Handle other data types as needed
                    throw new IllegalArgumentException("Unsupported data type: " + type);
                }
            } catch (Exception e) {
                return null;
            }
        }

        

        @Override
        public List<Object[]> evaluate(StorageManager storage_manager) {
            String tableName = getTableName();
            toPrint();
            /* Write your code here */
            try{

                if(!storage_manager.check_file_exists(tableName)){
                    return null;
                }
                int block_id = 1;
                List<Object[]> result = new ArrayList<Object[]>();
                List<Object[]> data = new ArrayList<Object[]>();
                List<RelDataTypeField> fields = rowType.getFieldList();
                String a = filter.toString();
                int id = a.indexOf("(");
                String op = a.substring(0, id);
                RexCall call = ((RexCall) filter);
                RexNode firstOperand = call.operands.get(0);
                int cidx = Integer.parseInt(firstOperand.toString().substring(1));
                RexNode literalValue = call.operands.get(1);
                Object val = createFieldValue(fields.get(cidx).getType(),literalValue.toString());

                while((data = storage_manager.get_records_from_block(tableName, block_id)) != null){
                    for(Object[] row : data){
                        if(evaluateCondition(op, row[cidx], val)){
                            Object[] projectedRow = new Object[projects.size()];
                            for(int idx = 0; idx < projects.size(); idx++){
                                String rexString = projects.get(idx).toString();
                                int columnIndex = Integer.parseInt(rexString.substring(1));
                                projectedRow[idx] = row[columnIndex];
                                

                            }
                            result.add(projectedRow);
                        }
                    }
                    block_id++;
                }
                return result;

            }catch(Exception e){
                return null;
            }
        }

        public void toPrint() {
            for (int i = 0; i < projects.size(); i++) {
                String rexString = projects.get(i).toString();
                // Assuming the string representation is of the form "$<index>"
                int index = Integer.parseInt(rexString.substring(1));
            }

            List<RelDataTypeField> fields = rowType.getFieldList();
            for (RelDataTypeField field : fields) {
                String columnName = field.getName();
                RelDataType columnType = field.getType();
            }

        }
}