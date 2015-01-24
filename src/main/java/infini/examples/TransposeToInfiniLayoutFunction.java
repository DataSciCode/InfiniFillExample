package infini.examples;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.operation.Function;

public class TransposeToInfiniLayoutFunction extends BaseOperation 
		implements Function  {

	
	 HashMap<String, String> rowKeyConstructMap;
	  String colValue = "";
	 public TransposeToInfiniLayoutFunction(HashMap<String, String> rowKeyConstructMap) throws Exception {
	        this.rowKeyConstructMap = rowKeyConstructMap;
	        
 
	 }
	 

	 


	public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
		// TODO Auto-generated method stub
		  TupleEntry inputTupleEntry = functionCall.getArguments();
		  
		  
	      HashMap<String, Integer> fieldMap = new HashMap<String, Integer>();
	        for (int i = 0; i < inputTupleEntry.getFields().size(); i++) {
	            fieldMap.put(inputTupleEntry.getFields().get(i).toString(), i);
	        }
		  // Construct row key
	       
	   
	       StringBuilder rowKeyBuilder = new StringBuilder();
	     String[] rowKeyFields = rowKeyConstructMap.get("FIELDS-CSV").split(",");
	        String rowkeyDelimiter = (rowKeyConstructMap.get("DELIMITER") == null ? "." : rowKeyConstructMap.get("DELIMITER"));
	        String rowkeyPrefix = (rowKeyConstructMap.get("PREFIX") == null ? "" : rowKeyConstructMap.get("PREFIX"));
	        String rowkeySuffix = (rowKeyConstructMap.get("SUFFIX") == null ? "" : rowKeyConstructMap.get("SUFFIX"));

	        if (rowkeyPrefix.length() > 0) {
	            rowKeyBuilder.append(rowkeyPrefix).append(rowkeyDelimiter);
	        }
	        for (String rowKeyField : rowKeyFields) {
	            rowKeyBuilder.append(
	                    inputTupleEntry.getString(fieldMap.get(rowKeyField)))
	                    .append(rowKeyConstructMap.get("DELIMITER"));
	        }
	        if (rowkeySuffix.length() > 0) {
	            rowKeyBuilder.append(rowkeySuffix).append(rowkeyDelimiter);
	        }
	        
	 
	        
	        String colRowKey = rowKeyBuilder.toString();
	        colRowKey = colRowKey.substring(0, colRowKey.length() - 1);
	       
 
	         
	        
	        
	        
		// Transpose and add result tuple to output collector
	        // {
	        for (int i = 0; i < inputTupleEntry.getFields().size(); i++) {

 
	            colValue = inputTupleEntry.getString(i);

	            Tuple resultTuple = new Tuple();
	            resultTuple.add(colRowKey);
 
	            resultTuple.add(colValue);
	            functionCall.getOutputCollector().add(resultTuple);
 
	            colValue = "";
 

	        }
	        // }
	}
}