package infini.examples;

import java.util.HashMap;
import java.util.Properties;

import org.apache.hadoop.mapred.JobConf;
import org.narahari.InfiniScheme;
import org.narahari.InfiniTap;

import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.operation.Identity;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.Discard;
import cascading.property.AppProps;
import cascading.scheme.hadoop.TextDelimited;
import cascading.scheme.hadoop.TextLine;
import cascading.tap.MultiSourceTap;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.GlobHfs;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;

 
/**
 * Hello world!
 *
 */
public class InfiniCacheFill 
{
	
	 public static void main(String[] args) throws Exception {

	        // {{
	        // Basic validation
	        if (args.length < 9) {
	            throw new Exception("Insufficient parameters!  "
	                    + "(1)HDFS input source, "
	                    + "(2)has header flag (Y/N), "
	                    + "(3)Input source field delimiter, "
	                    + "(4)input fields declarator, "
	                    + "(5)input discard fields declarator, "
	                    + "(6)output fields declarator, "
	                    + "(7)rowKey construct, "
	                    + "(8)output Infini connection string, and "
	                    + "(9)HDFS path for failures "
	                    + "are the required parameters!");
	        }
	        // }} 

	 
	 String HDFSInputSourcePath = args[0].toString();
     String hasHeader = args[1].toString();
     String inputSourceFieldDelimiter = args[2].toString();
     String inputSourceFieldDeclaratorCSV = args[3].toString();
     String inputSourceDiscardFieldDeclaratorCSV = args[4].toString();
     String inputSourceKeepFieldDeclaratorCSV = args[5].toString();
     String rowKeyConstructCSV = args[6].toString();
     String infiniConnectionString = args[7].toString();
     String trapHDFSPath = args[8].toString();
     
     // {{
     // JOB 
     JobConf jobConf = new JobConf();
     jobConf.setJarByClass(InfiniCacheFill.class);

     Properties properties = AppProps.appProps()
             .setName("ImportIntoInfinispan").setVersion("1.0.0")
             .buildProperties(jobConf);
     // }}
     
     
     HashMap<String, String> inputSourceRowKeyConstructMap = new HashMap<String, String>();
     
     boolean rowKeyInfoValid = false;
     
     if (rowKeyConstructCSV.length() >= 1)
     {
         if (Util.getMap("", rowKeyConstructCSV).size() > 0) {
             inputSourceRowKeyConstructMap
                     .putAll(Util
                             .getMap("", rowKeyConstructCSV));
             rowKeyInfoValid = true;
         }
     }

     
     //Instantiate field list
     Fields inputSourceFieldsDeclarator = new Fields();
     boolean inputSourceFieldsInfoValid = false;
     if (inputSourceFieldDeclaratorCSV.length() > 1) 
     {
         inputSourceFieldsDeclarator = inputSourceFieldsDeclarator.append(Util.buildFieldList(inputSourceFieldDeclaratorCSV));
         if (inputSourceFieldsDeclarator.size() > 0) {
             inputSourceFieldsInfoValid = true;
         }
     }

     Fields discardInputFieldsDeclarator = new Fields();
     boolean inputSourceDiscardFieldsInfoValid = false;
     if (inputSourceDiscardFieldDeclaratorCSV.length() > 1)
     {
         discardInputFieldsDeclarator = discardInputFieldsDeclarator.append(Util.buildFieldList(inputSourceDiscardFieldDeclaratorCSV));
         if (discardInputFieldsDeclarator.size() > 0) {
             inputSourceDiscardFieldsInfoValid = true;
         }
     }
     
     Fields inputSourceKeepFieldsDeclarator = new Fields();
     boolean inputSourceKeepFieldsInfoValid = false;
     if (inputSourceKeepFieldDeclaratorCSV.length() > 1) 
     {
         inputSourceKeepFieldsDeclarator = inputSourceKeepFieldsDeclarator.append(Util.buildFieldList(inputSourceKeepFieldDeclaratorCSV));
         if (inputSourceKeepFieldsDeclarator.size() > 0) {
             inputSourceKeepFieldsInfoValid = true;
         }
     }
     
     
     
     // Further validation
     if (!rowKeyInfoValid || !inputSourceFieldsInfoValid) {
         StringBuilder errBuilder = new StringBuilder();

         if(!hasHeader.equals("Y") && !hasHeader.equals("N")){
             errBuilder.append("Invalid value for \"has header\" flag;");
         }
         if (inputSourceFieldDelimiter.trim().length() == 0) {
             errBuilder.append("Input source field delimiter parameter is required;");
         }
         if (!inputSourceFieldsInfoValid) {
             errBuilder.append("Output fields parameter construct is invalid;");
         }
         if (!rowKeyInfoValid) {
             errBuilder.append("Row key parameter construct is invalid;");
         }
 
         throw new Exception("The following issues were found for the parameters provided: "
                 + errBuilder.toString());
     }
     
     // SOURCE tap - HDFS
     HadoopFlowProcess hfp = new HadoopFlowProcess(jobConf);

  
	TextDelimited sourceSchemeData = new TextDelimited(
             inputSourceFieldsDeclarator, (hasHeader.equals("Y") ? true : false),inputSourceFieldDelimiter);
     GlobHfs sourceFilesGlobData = new GlobHfs(sourceSchemeData,
             HDFSInputSourcePath);
     Tap sourceTapHDFS = new MultiSourceTap(sourceFilesGlobData);
     // }}
     
     // {{
     // SINK tap - Infinispan
     InfiniTap sinkTapInfini = new InfiniTap(infiniConnectionString,
             new InfiniScheme(), SinkMode.UPDATE);
	// }}

     // {{
     // TRAP tap - HDFS
     Tap sinkTrapTapHDFS = new Hfs(new TextLine(), trapHDFSPath,
             SinkMode.REPLACE);
	// }}

     // {{
     // PIPE
     Pipe rawDataPipe = new Pipe("rawDataPipe");
     
     rawDataPipe = new Each(rawDataPipe, new Identity(
             inputSourceFieldsDeclarator));

     if (inputSourceDiscardFieldsInfoValid) {
         rawDataPipe = new Discard(rawDataPipe,discardInputFieldsDeclarator);
     }
     
 
     Pipe transformPipe = new Pipe("transformPipe");
     
     // Transpose data, create key,  
     transformPipe = new Each(rawDataPipe,       
                                inputSourceKeepFieldsDeclarator, 
                                new TransposeToInfiniLayoutFunction(
                                     inputSourceRowKeyConstructMap), 
                     Fields.RESULTS);

     transformPipe = new Each(transformPipe, new Identity(
             sinkTapInfini.getDefaultInfiniFields()));

 
	// }}

     // {{
     // EXECUTE
     // Connect the taps, pipes, etc., into a flow & execute
     HadoopFlowConnector flowConnector = new HadoopFlowConnector(properties);

     FlowDef flowDef = FlowDef
             .flowDef()
             .setName("InfiniImport-flow-def")
             .addSource(rawDataPipe, sourceTapHDFS)
             .addTrap(rawDataPipe, sinkTrapTapHDFS)
              .addTailSink(transformPipe, sinkTapInfini);
 
     Flow flow = flowConnector.connect(flowDef);
     flow.complete();
     // }}
	 }  
}

    
