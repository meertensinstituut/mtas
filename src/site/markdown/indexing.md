#Indexing

To the existing Lucene approach of creating and searching indexes we add annotation and structure by using prefixes to distinguish between text and the different annotations and we use the payload to encode this additional information. By implementing the additions to the indexing process as an extension of the default Lucene Codec, updating and deleting documents is still supported, just as merging and optimizing cores.

**Configuration**

The use of prefixes provides a direct solution to store and search for annotations on separate words within a text, and only an adjusted tokenizer is needed to offer the correct tokenstream to the indexer. To be able to store ranges of words (e.g. sentences, paragraphs, chapters), distinct sets of words (e.g. named entities), and hierarchical relations between all of these annotations or words, we encode additional information as an array of bytes and store it as payload. 

Several parsers are available for different [formats](indexing_formats.html), and for each of these parsers extensive [configuration](indexing_configuration.html) of the mapping can be provided. Furthermore, multiple document formats and/or mapping configurations can be used within the same core. 

