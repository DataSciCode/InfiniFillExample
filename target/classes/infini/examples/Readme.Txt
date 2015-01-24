Readme.Txt
++++++++++++++++++++++++++++++++++++++
The sample (very basic) program in the “src” directory demonstrates how to read data in a flat delimited file with a header, 
in HDFS  and save to Infinispan - using Cascading and Cascading extensions for Infinispan.


Input data format
=================================================
Book_ID,Name,Published
b002,A sign of four,1890
b004,The memoirs of Sherlock Holmes,1893
b006,The return of Sherlock Holmes,1905


Input fields & output fields
=================================================
Input=["Book_ID","Name","Published"]
Output=["rowID", "colF", "colQ", "colVis", "colTimestamp", "colVal”]
