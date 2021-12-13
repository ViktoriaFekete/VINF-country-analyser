import re
import xml.etree.ElementTree as ET
import HTMLParser
import pyspark
from pyspark.sql.session import SparkSession
from pyspark.context import SparkContext
from pyspark.conf import SparkConf
from pyspark.sql.functions import col


def remove_chars(text):
    list_of_chars = [']', '[', '}', '{', '(', ')', '\n', '\t',"'",',']
    pattern = '[' + ''.join(list_of_chars) + ']'
    text = re.sub(pattern, '', text)
    text = text.strip()
    return text


def remove_strings(text):
    text = text.replace('IncreaseNeutral','')
    text = text.replace('DecreaseNeutral','')
    text = text.replace('IncreasePositive','')
    text = text.replace('DecreasePositive','')
    text = text.replace('IncreaseNegative','')
    text = text.replace('DecreaseNegative','')
    text = text.replace('increase','')
    text = text.replace('Increase','')
    text = text.replace('Decrease','')
    text = text.replace('decrease','')
    text = text.replace('hidden','')
    text = text.replace('|', ' ')
    text = re.sub('[a-zA-Z]* ?list', '', text)
    text = text.strip()
    return text


def remove_html_tags(raw_html):
    tag_regex = re.compile('<.*?>')
    clean_text = re.sub(tag_regex, '', raw_html)
    return clean_text


def remove_keyword(text):
    keywords = ['capital','currency','GDP_PPP']
    for keyword in keywords:
        keyword_regex = re.compile('\|? ?'+keyword+'\s*=')
        clean_text = re.sub(keyword_regex, '', text)
        text = clean_text
    return clean_text


def clean_text(text_list):
    encoded_list = []
    for text in text_list:
        if (type(text) == tuple):
            for tuple_item in text:
                if len(tuple_item) > 0:
                    encoded_list.append(tuple_item.encode('utf-8'))
        else:
            encoded_list.append(text.encode('utf-8'))

    text = str(encoded_list)
    text = remove_keyword(text)
    text = remove_chars(text)
    text = remove_strings(text)
    return text


def clean_raw_line(line):
    line = HTMLParser.HTMLParser().unescape(line)
    line = remove_html_tags(line)
    return line


def clean_lang_arr(lang_arr):
    encoded_lang_arr = []
    for lang in lang_arr:
        encoded_lang_arr.append(lang.encode('utf-8'))

    exceptions = ['Official','Two','The','These','Minority','National','Other','Common','If', 'Regional','Native','Spoken', 'Endangered','First','Last','Second','What','Local','Recognised','Known']

    for ex in exceptions:
        ex_lang = ex + ' language'
        ex_langs = ex + ' languages'
        if ex_lang in encoded_lang_arr:
            encoded_lang_arr.remove(ex_lang)
        elif ex_langs in encoded_lang_arr:
            encoded_lang_arr.remove(ex_langs)

    return encoded_lang_arr


def str_to_num(text):
    text = text.replace('$','')
    text = text.replace('nowrap','')
    num = re.findall('[0-9]+\.?[0-9]*', text)
    if len(num) > 0:
        if str(num[0]) == text:
            return float(num[0])
        elif 'billion' in text:
            return float(num[0]) * 1000000000
        elif 'million' in text:
            return float(num[0]) * 1000000
        elif 'trillion' in text:
            return float(num[0]) * 1000000000000
    else:
        return 0


def get_specific_data_from_article(text):
    country_dict = {}

    name_regex = '\| ?common_name\s*= ?(\w+ |\w+)+'
    capital_regex = "\| ?capital\s*= ?[\[\w+\]\|\.\', ]+"
    languages_regex = "\| ?languages\s*= ?[\{\w+\}]*[\[\]\w+, \|\.\$\n\{\*\(\)]+[\}]?"
    lang_regex = "[A-Z][a-z]+ language"
    ethnic_regex = '\| ?(ethnic_groups)\s*= ?(\{\{\w+\}\})*(\[|\]|\w+|,| |\||\.|\$|%|-|\n|\{|\*)+(\}\})?'
    religion_regex = '([0-9.]+ ?\%)+ ?((\[| |[a-z-]|[a-zA-Z]\|[a-zA-Z])+\]\])+(\w+| )*(\}\})?'
    population_est_regex = '\| ?population_estimate\s*= ?(\{\{\w+\}\})*(\[|\]|[0-9.,]+|,| |\|)+'
    population_census_regex = '\| ?population_census\s*= ?(\{\{\w+\}\})*(\[|\]|[0-9.,]+|,| |\|)+'
    area_regex ='\| ?area_km2\s*= ?(\{\{\w+\}\})*(\[|\]|[0-9.,]+|\.|,| |\|)+'
    GDP_regex = "\| ?GDP_PPP\s*= ?[\{\w+\}]*[\[\]0-9.,\|\.|$ ]+ ?[\w+]*"
    currency_regex = "\| ?currency\s*= ?[\{\}\w+]*[\[\]\w+,\.\$ ]+"

    # for line in lines:
    line = clean_raw_line(text)

    if re.findall(name_regex, line):
        matching_string = re.findall(name_regex, line)
        country_dict['name'] = clean_text(matching_string)

    if re.findall(capital_regex, line):
        matching_string = re.findall(capital_regex, line)
        country_dict['capital'] = clean_text(matching_string)

    if re.findall(population_est_regex, line):
        matching_string = re.findall(population_est_regex, line)
        country_dict['population'] = clean_text(matching_string)
        country_dict['population'] = str_to_num(country_dict['population'])
    elif re.findall(population_census_regex, line):
        matching_string = re.findall(population_census_regex, line)
        country_dict['population'] = clean_text(matching_string)
        country_dict['population'] = country_dict['population'].replace(',','')
        country_dict['population'] = str_to_num(country_dict['population'])


    if re.findall(area_regex, line):
        matching_string = re.findall(area_regex, line)
        matching_string = list(set(matching_string))
        country_dict['area'] = clean_text(matching_string)
        country_dict['area'] = country_dict['area'].replace(',','')
        country_dict['area'] = str_to_num(country_dict['area'])

    if re.findall(GDP_regex, line):
        matching_string = re.findall(GDP_regex, line)
        country_dict['gdp'] = clean_text(matching_string)
        country_dict['gdp'] = country_dict['gdp'].replace(',','')
        country_dict['gdp'] = str_to_num(country_dict['gdp'])

    # if re.findall(religion_regex, line):
    #     country_dict['religion'] = re.findall(religion_regex, line)

    if re.findall(currency_regex, line):
        matching_string = re.findall(currency_regex, line)
        country_dict['currency'] = clean_text(matching_string)

    if re.findall(languages_regex, line):
        matching_string = re.findall(lang_regex, line)
        lang_arr = list(set(matching_string))
        lang_arr = clean_lang_arr(lang_arr)
        country_dict['languages'] = lang_arr


    # if re.findall(ethnic_regex, line):
    #     matching_string =  re.findall(ethnic_regex, line)
    #     country_dict['ethnic_groups'] = clean_text(matching_string)

    country_dict = dict(sorted(country_dict.items()))

    return country_dict


def filter_countries(title, text):
    if "history" not in title.lower() and "demographics" not in title.lower() and "geography" not in title.lower() \
     and "government" not in title.lower() and "template" not in title.lower()  and "list" not in title.lower()\
         and "abbey" not in title.lower() and "special" not in title.lower() and "university" not in title.lower():

        if re.findall('Infobox country', text):
            return title, text
        else:
            return None
    else:
        return None



def main():

    # distribution of whole wiki dump
    print("\n>> C R E A T I N G   S P A R K S E S S I O N\n")
    sc = SparkContext.getOrCreate()
    spark = SparkSession(sc)
    print(spark)

    print("\n>> L O A D I N G  W I K I  XML\n")
    df = spark.read.format("com.databricks.spark.xml").options(rowTag="page").load("enwiki-latest-pages-articles1.xml-p1p41242")

    rdd_selected = df.select("title", pyspark.sql.functions.col("revision.text._VALUE").alias("text"))\
        .dropna(how='any')\
        .rdd\
        .map(lambda row: filter_countries(row['title'], row['text']))\
        .filter(lambda row: row is not None)\
        .map(lambda row: (row[0], get_specific_data_from_article(row[1])))\
        .groupByKey()\
        .mapValues(list)\
        .saveAsTextFile('rdd_edited_wiki')

    # print("\n ------------------ DF SELECTED ----------------->>\n")
    for country in rdd_selected:
        print(country, '\n')
    # print("\n <<---------------- DF SELECTED -----------------\n")


main()
