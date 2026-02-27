package com.example;

import org.apache.commons.text.StringTokenizer;
import org.apache.hadoop.io.IntWritable;

//This area of our code breaks down the document into KEY VALUE pairs.
//Use Split in the document to showcase and determine the key-value pairs for the strings. 
public class DocumentSimilarityMapper extends Mapper<Object, Text, Text, Text> {

    private final static IntWritable one = new IntWritable(1);

    private Text documentId = new Text();
    private Text outputValues = new Text();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer itr = new StringTokenizer(value.toString());
        
        String line = value.toString();

        String[] parts = line.split("\\s+", 2);

        if(parts.length == 2){
            documentId.set(parts[0]);
            outputValues.set(parts[1]);
            context.write(new Text(documentId), new Text(outputValues));
        }
    }
}
