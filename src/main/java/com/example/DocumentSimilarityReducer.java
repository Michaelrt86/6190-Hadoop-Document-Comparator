package com.example;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.*;
import org.apache.commons.text.StringTokenizer;
import org.apache.hadoop.io.IntWritable;

//The reducer takes the KEY VALUE pairs from the mapper and then calculates the similarity score using Jaccard similarity.
public class DocumentSimilarityReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // Collect all documents with their word sets
        Map<String, Set<String>> docs = new LinkedHashMap<>();

        for (Text val : values) {
            String line = val.toString();
            String[] parts = line.split("\t", 2);
            if (parts.length < 2) {
                continue;
            }
            String docId = parts[0];
            String content = parts[1];

            // Tokenize content and store unique words (lowercase)
            String[] tokens = content.split("\\s+");
            Set<String> words = new HashSet<>();
            for (String token : tokens) {
                if (!token.isEmpty()) {
                    words.add(token.toLowerCase());
                }
            }
            docs.put(docId, words);
        }

        // Compute pairwise Jaccard similarity between all document pairs
        List<String> docIds = new ArrayList<>(docs.keySet());
        for (int i = 0; i < docIds.size(); i++) {
            for (int j = i + 1; j < docIds.size(); j++) {
                String id1 = docIds.get(i);
                String id2 = docIds.get(j);
                Set<String> set1 = docs.get(id1);
                Set<String> set2 = docs.get(id2);

                // Compute intersection
                Set<String> intersection = new HashSet<>(set1);
                intersection.retainAll(set2);

                // Compute union
                Set<String> union = new HashSet<>(set1);
                union.addAll(set2);

                // Calculate Jaccard similarity
                double similarity = 0.0;
                if (!union.isEmpty()) {
                    similarity = (double) intersection.size() / union.size();
                }

                // Format output as "Document1, Document2 Similarity: 0.XX"
                String formatted = String.format("%.2f", similarity);
                String output = id1 + ", " + id2 + " Similarity: " + formatted;
                context.write(new Text(output), new Text(""));
            }
        }
    }
}
