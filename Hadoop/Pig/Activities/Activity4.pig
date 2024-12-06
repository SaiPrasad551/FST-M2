-- Load the file into pig
inputFile = LOAD '/root/input.txt' AS (line:chararray);
-- Tokeize the lines into words
words = FOREACH inputFile GENERATE FLATTEN(TOKENIZE(line)) AS word;
-- Combine the words from the above stage
grpd = GROUP words BY word;
-- Count the occurence of each word (Reduce)
cntd = FOREACH grpd GENERATE $0 AS word, COUNT($1) AS no_of_words;

--To remove old outputs
rmf /root/pigOutput1;
-- Store the result in HDFS
STORE cntd INTO '/root/PigOutput1';
