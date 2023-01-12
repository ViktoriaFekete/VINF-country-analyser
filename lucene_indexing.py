# Base lucene code: 
# https://vi2021.ui.sav.sk/lib/exe/fetch.php?media=11_seleng_ir_tools.pdf

import lucene
import re
from java.nio.file import Paths
from org.apache.lucene.analysis.standard import StandardAnalyzer
from org.apache.lucene.index import IndexWriter, IndexWriterConfig
from org.apache.lucene.document import Document, Field, TextField, FloatPoint
from org.apache.lucene.store import SimpleFSDirectory

from org.apache.lucene.search import IndexSearcher
from org.apache.lucene.index import DirectoryReader
from org.apache.lucene.queryparser.classic import QueryParser

import io
import ast
import os

    
lucene.initVM(vmargs=['-Djava.awt.headless=true'])

store = SimpleFSDirectory(Paths.get("tempIndex"))
analyzer = StandardAnalyzer()
config = IndexWriterConfig(analyzer)
config.setOpenMode(IndexWriterConfig.OpenMode.CREATE)
writer = IndexWriter(store, config)

folder = 'rdd_wiki_edited/'
# folder = 'rdd_edited'
country_list = []

for filename in os.listdir(folder):
    if filename.startswith("part-"):      
        rdd_file = io.open(folder+'/'+filename, 'r', encoding='utf-8')

    mylist = []
    for line in rdd_file:
        doc = Document()
        
        s = ast.literal_eval(str(line))
        country_name = s[0]
        doc.add(Field("country_name", str(country_name), TextField.TYPE_STORED))
        country_list.append(country_name)

        if type(s[1]) == list:
            country_data = s[1]
            content = []
            for keyword, value in country_data[0].items():
                doc.add(Field(keyword, str(value), TextField.TYPE_STORED))
                content.append(value)
            doc.add(Field("content", str(content), TextField.TYPE_STORED))
        writer.addDocument(doc)

writer.commit()
writer.close()
    
directory = SimpleFSDirectory(Paths.get("tempIndex"))
searcher = IndexSearcher(DirectoryReader.open(directory))
analyzer = StandardAnalyzer()

command_dict = {
    "-curr": "currency",
    "-cap": "capital",
    "-country":"country_name",
    "-lang" : "languages",
    "-all":"content",
    "-gdp":"gdp",
    "-area":"area",
    "-pop":"population"
}

print("\nC O U N T R Y   A N A L Y Z E R\n")
print("You can search for specific information using the following commands")
print("-country -- country name\n-curr -- currency\n-cap -- capital\n-lang -- languages\n-gdp -- gdp\n-area -- area\n-pop -- population\n-all -- whole content\n-exit")

query_input = input("\nSearch for: ")

while query_input != '-exit':
    query_command = query_input.split(' ', 1)[0]
    query_term = query_input.split(' ', 1)[1]

    query = QueryParser("content", analyzer).parse("search " + query_term)
    scoreDocs = searcher.search(query, 150).scoreDocs

    print("%s total matching documents." % len(scoreDocs))
    for scoreDoc in scoreDocs:
        doc = searcher.doc(scoreDoc.doc)
        answer = doc.get(command_dict[query_command if query_command else "country_name"])
        if answer is not None and len(answer)>0:
            print(query_command,": ", answer)

    query_input = input("\nSearch for: ")

