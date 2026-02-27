package com.example;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

//This area of our code breaks down the document into KEY VALUE pairs.
//Use Split in the document to showcase and determine the key-value pairs for the strings. 
public class DocumentSimilarityMapper extends Mapper<Object, Text, Text, Text> {

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString().trim();
        
        if (line.isEmpty()) {
            return;
        }

        String[] parts = line.split("\\s+", 2);

        if(parts.length == 2){
            String docId = parts[0];
            String content = parts[1];
            // Create new Text object for constant key each time
            context.write(new Text("ALL"), new Text(docId + "\t" + content));
        }
    }
}\n
