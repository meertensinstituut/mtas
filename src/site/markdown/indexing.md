#Indexing

To the existing Lucene approach of creating and searching indexes we add annotation and structure by using prefixes to distinguish between text and the different annotations and we use the payload to encode this additional information. The use of prefixes provides a direct solution to store and search for annotations on separate words within a text, and only an adjusted tokenizer is needed to offer the correct tokenstream to the indexer. To be able to store ranges of words (e.g. sentences, paragraphs, chapters), distinct sets of words (e.g. named entities), and hierarchical relations between all of these annotations or words, we encode this information as an array of bytes and store it as payload. 


