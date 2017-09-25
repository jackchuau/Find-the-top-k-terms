# Find-the-top-k-terms
Find the top-k terms that appear in the most lines

Given a large text file, each term is contained in several lines. Your task is to find the top-k terms that appear in the most lines.

Requirments:
* Ignore the letter case, i.e., consider all words as lower case.
* Ignore terms starting with non-alphabetical characters, i.e., only
consider terms starting with “a” to “z”.
* Use the following split function to split the documents into terms:
split("[\\s*$&#/\"'\\,.:;?!\\[\\](){}<>~\\-_]+")

Output format:
Your Spark program should generate a list of k key-value pairs, where the keys are the terms, the values are the number of lines containing the term, and keys and values are separated by “\t”. The key-value pairs are sorted in descending order according to the values.

Code format:
Name your scala file as “Problem1.scala”, the object as “Problem1”, and put it in package “comp9313.ass3”. Store the final result in a text file on disk. Your program should take three parameters: the input text file, the output folder, and the value of k.


