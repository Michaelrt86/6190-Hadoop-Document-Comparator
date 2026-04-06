# Assignment 2: Document Similarity using MapReduce

**Name:** 
Michael Thompson


## Approach and Implementation

### Mapper Design
[Explain the logic of your Mapper class. What is its input key-value pair? What does it emit as its output key-value pair? How does it help in solving the overall problem?]

### Reducer Design
[Explain the logic of your Reducer class. What is its input key-value pair? How does it process the values for a given key? What does it emit as the final output? How do you calculate the Jaccard Similarity here?]

### Overall Data Flow
[Describe how data flows from the initial input files, through the Mapper, shuffle/sort phase, and the Reducer to produce the final output.]

---

## Setup and Execution

### ` Note: The below commands are the ones used for the Hands-on. You need to edit these commands appropriately towards your Assignment to avoid errors. `

### 1. **Start the Hadoop Cluster**

Run the following command to start the Hadoop cluster:

```bash
docker compose up -d
```

### 2. **Build the Code**

Build the code using Maven:

```bash
mvn clean package
```

### 4. **Copy JAR to Docker Container**

Copy the JAR file to the Hadoop ResourceManager container:

```bash
docker cp target/DocumentSimilarity-0.0.1-SNAPSHOT.jar resourcemanager:/opt/hadoop-3.2.1/share/hadoop/mapreduce/
```

### 5. **Move Dataset to Docker Container**

Copy the dataset to the Hadoop ResourceManager container:

```bash
docker cp shared-folder/input/data/input.txt resourcemanager:/opt/hadoop-3.2.1/share/hadoop/mapreduce/
```

### 6. **Connect to Docker Container**

Access the Hadoop ResourceManager container:

```bash
docker exec -it resourcemanager /bin/bash
```

Navigate to the Hadoop directory:

```bash
cd /opt/hadoop-3.2.1/share/hadoop/mapreduce/
```

### 7. **Set Up HDFS**

Create a folder in HDFS for the input dataset:

```bash
hadoop fs -mkdir -p /input/data
```

To see the folder after creation use this command (ls is a linux command and does not show Hadoop file system)
```bash
hadoop fs -ls /
```

Copy the input dataset to the HDFS folder:

```bash
hadoop fs -put ./input.txt /input/data
```

### 8. **Execute the MapReduce Job**

Run your MapReduce job using the following command: Here I got an error saying output already exists so I changed it to output1 instead as destination folder

```bash
hadoop jar DocumentSimilarity-0.0.1-SNAPSHOT.jar com.example.controller.Controller /input/data/input.txt /output1
```

### 9. **View the Output**

To view the output of your MapReduce job, use:

```bash
hadoop fs -cat /output1/*
```

### 10. **Copy Output from HDFS to Local OS**

To copy the output from HDFS to your local machine:

1. Use the following command to copy from HDFS:
    ```bash
    hdfs dfs -get /output1 /opt/hadoop-3.2.1/share/hadoop/mapreduce/
    ```

2. use Docker to copy from the container to your local machine:
   ```bash
   exit 
   ```
    ```bash
    docker cp resourcemanager:/opt/hadoop-3.2.1/share/hadoop/mapreduce/output1/ shared-folder/output/
    ```
3. Commit and push to your repo so that we can able to see your output


---

## Challenges and Solutions

[Describe any challenges you faced during this assignment. This could be related to the algorithm design (e.g., how to generate pairs), implementation details (e.g., data structures, debugging in Hadoop), or environmental issues. Explain how you overcame these challenges.]<br>

Many of the challenges I had faced in this assignment were due to misunderstandings of Hadoop and how it worked. Overall I was fairly confused with how the Mapper and Reducer function would be able to work together. I also learned more about datatypes like Text and used a StringTokenizer (which I don't often do). My solution also used a Hashset which is essentially a Hashmap but only contains objects and not Key-value pairs (Co-pilot suggestion that I learned more about from GeeksforGeeks).<br>

Co-Pilot helped me debug in areas that I began to lack and really helped me understand how this works overall. I do not want to become dependent upon an AI development tool but it really helped guide my understanding of this assignment. Before I was having a lot of different issues with outputting and it helped clear some of my code and some of the bugs that were stopping me. I am not used to programming with AI but I can understand the application of it further.<br>

Using the Hadoop commands also threw me off earlier on since it is a different file system so traditional ```ls``` would not work but instead I needed to run ```hadoop fs -ls /``` since I was not viewing a traditional file system. I had to start from scratch and then I reunderstood my previous logic from before. I had to look into how Jaccard Similarity worked and used GeeksForGeeks as a reference to get a better understanding of it. I had never used anything like it but it was really interesting to look into. 

---
## Sample Input

**Input from `small_dataset.txt`**
```
Document1 This is a sample document containing words
Document2 Another document that also has words
Document3 Sample text with different words
```
## Sample Output

**Output from `small_dataset.txt`**
```
"Document1, Document2 Similarity: 0.56"
"Document1, Document3 Similarity: 0.42"
"Document2, Document3 Similarity: 0.50"
```
## Obtained Output: (Place your obtained output here.)

**My obtained output from 'input.txt' was***
```
Document3, Document2 Similarity: 0.10
Document3, Document1 Similarity: 0.20
Document2, Document1 Similarity: 0.18
```