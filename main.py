import collections
import re
import xml.etree.ElementTree as ET
import itertools
#import nltk
import html
import HTMLParser
import io
import pyspark
from pyspark.sql.session import SparkSession
from pyspark.context import SparkContext
from pyspark.conf import SparkConf
from pyspark.sql.functions import col

def get_country_pages(file):
    context = ET.iterparse(file, events=('end',))
    index = 0
    files = {}
    for event, elem in context:
        if elem.tag == '{http://www.mediawiki.org/xml/export-0.10/}page':
            elem_str = ET.tostring(elem, encoding='utf8').decode("utf-8")

            if re.findall('Infobox country(\n|\|)', elem_str):
                # print(ET.tostring(elem))
                index += 1
                filename = "240MB\\" + format(str(index) +".csv")
                files[index] = filename
                with open(filename, 'wb') as f:
                    # print(ET.tostring(elem))
                    f.write(ET.tostring(elem))
    return files


def clean_data(line):
    line = line.replace('[', '')
    line = line.replace(']', '')
    line = line.replace('{', '')
    line = line.replace('}', '')
    line = line.replace('=', '')
    line = line.replace('sq_mi', '')
    line = line.split('&lt;ref')[0]
    line = line.replace('&amp;nbsp;', ' ')
    line = line.replace('_code ', '')
    line = line.replace('{{IncreaseNeutral}}','')
    line = line.replace('{{increase}}','')
    line = line.replace('IncreasePositive','')
    line = line.replace('IncreaseNeutral','')
    line = line.replace('DecreaseNeutral','')
    line = line.replace('DecreasePositive','')
    line = line.replace(' of ', ' ')
    line = line.replace(' in ', ' ')
    line = line.replace('increase', ' ')
    line = line.replace('decrease', ' ')
    line = line.replace('| languages','')
    line = line.replace('Languages','')
    line = line.replace('|See full list','')
    line = line.replace('List:','')
    line = line.replace('<br />', '')
    line = line.replace('<br/>', '')
    line = line.replace('<sup>a</sup>', '')
    # line = html.unescape(line)
    line = line.strip()
    return line


def clean_array(line):
    line = line.replace('{{figure space}}','')
    line = line.replace('{{unbulleted list','')
    line = line.replace('{{Unbulleted list','')
    line = line.replace('{{ublist','')
    line = line.replace('{{vunblist','')
    line = line.replace('{{nowrap','')
    line = line.replace('{{plainlist|','')
    line = line.replace('| languages','')
    line = line.replace('{{small|','')
    line = line.strip()
    return line


def read_section(_file, keyword):
    section = ""
    count_start = 0
    count_end = 0

    lines = io.open(_file, 'r', encoding='utf-8').read()

    for line in lines:
        if re.findall(keyword, line):
            count_start += line.count('{')
            count_end += line.count('}')
            # line = clean_array(line)
            # line = clean_data(line)
            section += line

            while count_start > count_end:
                line = next(lines, '').strip()
                count_start += line.count('{')
                count_end += line.count('}')
                # line = clean_array(line)
                # line = clean_data(line)
                section += line
        if count_start != 0 and count_start == count_end:
            break

    return section


def lines_to_array(lines):
    lines = lines.replace('{','')
    lines = lines.replace('}','')

    if re.findall('plainlist', lines):
        lines = lines.replace('plainlist|','')
        lines = lines.replace('| languages','')
        lines = lines.replace('| ethnic_groups','')
        lines = lines.strip()
        array = lines.split('*')
        return array
    if re.findall('(unbulleted list|Unbulleted list|ublist|vunblist)', lines):
        lines = lines.replace('unbulleted list','')
        lines = lines.replace('Unbulleted list','')
        lines = lines.replace('ublist','')
        lines = lines.replace('vunblist','')
        lines = lines.replace('nowrap','')
        lines = lines.replace('| ethnic_groups','')
        lines = lines.replace('| religion','')
        lines = lines.replace('Demographics','others')
        lines = lines.replace('Ethnic','others')
        lines = lines.replace('}}','').strip()
        array = re.findall('(\d+\.?\d*% \w+)', lines)
        return array
    if re.findall('(hlist|\| official_languages)', lines):
        lines = lines.replace('hlist','')
        lines = lines.replace('| official_languages  ','')
        lines = lines.replace('}}','')
        lines = lines.replace(' language','')
        lines = lines.replace('s2','').strip()
        lines = lines.replace('  ',' ')
        array = [x.strip() for x in lines.split('|')]
        array = filter(None, array)
        array = set(array)
        return list(array)
    else:
        lines = lines.replace('| religion', '')
        lines = lines.replace('| ethnic_groups', '')
        lines = lines.replace('| official_languages', '')
        lines = lines.replace('*','|')
        lines = lines.strip()
        array = []
        if re.findall('[0-9]+\% ?\w+', lines):
            array = re.findall('[0-9]+\% ?\w+', lines)
        else:
            array = [x.strip() for x in lines.split('|')]
        return array


def get_specific_data_from_infobox(_file):
    country_dict = {}

    lines = io.open(_file, 'r', encoding='utf-8').read()

    # Get basic one-string data from infobox
    keywords_string = ['common_name', 'capital', 'currency', 'area_km2', 'population_estimate ', 'GDP_PPP ', '(languages |\| languages2 |\| official_languages)', 'ethnic_groups ', 'religion ']

    for keyword in keywords_string:
        for line in lines:
            if re.findall('\|.?' + keyword, line):
                if re.findall('{{\w+ ?list', line):
                    lines = read_section(_file, keyword)
                    lines = clean_data(lines)
                    lines = lines_to_array(lines)
                    if keyword == '(languages |\\| languages2 |\\| official_languages)':
                        keyword = 'language'
                    country_dict[keyword] = lines
                else:
                    line = line.replace('| ' + keyword, '').replace('|' + keyword, '').strip()
                    line = clean_data(line)
                    if keyword == '(languages |\\| languages2 |\\| official_languages)':
                        keyword = 'language'
                    country_dict[keyword] = line

                if country_dict[keyword] != None:
                    break
    # Get array-like data from infobox
    # keywords_array = ['(\| languages |\| languages2 |\| official_languages)', '\| ethnic_groups ', '\| religion ']
    # for keyword in keywords_array:
    #     key = keyword[3:-1]
    #     if key == ' languages |\\| languages2 |\\| official_languages':
    #         key = 'language'
    #     lines = read_section(_file, keyword)
    #     lines = lines_to_array(lines)
    #     country_dict[key] = lines

    return country_dict

# -------------------------------------------------------------------------------------------------------------------
def remove_chars(text):
    list_of_chars = [']', '[', '}', '{', '(', ')', '\n', '\t',"'",',']
    pattern = '[' + ''.join(list_of_chars) + ']'
    text = re.sub(pattern, '', text)
    text = text.strip()
    return text


def remove_strings(text):
    text = text.replace('increase','')
    text = text.replace('decrease','')
    text = text.replace('IncreaseNeutral','')
    text = text.replace('DecreaseNeutral','')
    text = text.replace('|', ' ')
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


def clean_text(text):
    text = str(text)
    text = remove_keyword(text)
    text = remove_chars(text)
    text = remove_strings(text)
    return text


def clean_raw_line(line):
    line = HTMLParser.HTMLParser().unescape(line)
    line = remove_html_tags(line)
    return line


def get_specific_data_from_article(text):
    country_dict = {}

    name_regex = '\| ?common_name\s*= ?(\w+ |\w+)+'
    capital_regex = "\| ?capital\s*= ?[\[\w+\]\|\.\', ]+"
    languages_regex = "\| ?languages\s*= ?[\{\w+\}]*[\[\]\w+, \|\.\$\n\{\*\(\)]+[\}]?"
    ethnic_regex = '\| ?(ethnic_groups)\s*= ?(\{\{\w+\}\})*(\[|\]|\w+|,| |\||\.|\$|\n|\{|\*)+(\}\})?'
    # religion_regex = '([0-9.]+ ?\%)+ ?((\[| |[a-z-]|[a-zA-Z]\|[a-zA-Z])+\]\])+(\w+| )*(\}\})?'
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
    elif re.findall(population_census_regex, line):
        matching_string = re.findall(population_census_regex, line)
        country_dict['population'] = clean_text(matching_string)

    if re.findall(area_regex, line):
        matching_string = re.findall(area_regex, line)
        country_dict['area'] = clean_text(matching_string)

    if re.findall(GDP_regex, line):
        matching_string = re.findall(GDP_regex, line)
        country_dict['gdp'] = clean_text(matching_string)

    # if re.findall(religion_regex, line):
    #     country_dict['religion'] = re.findall(religion_regex, line)

    if re.findall(currency_regex, line):
        matching_string = re.findall(currency_regex, line)
        country_dict['currency'] = clean_text(matching_string)

    if re.findall(languages_regex, line):
        matching_string = re.findall(languages_regex, line)
        country_dict['languages'] = clean_text(matching_string)

    if re.findall(ethnic_regex, line):
        matching_string =  re.findall(ethnic_regex, line)
        country_dict['ethnic_groups'] = clean_text(matching_string)

    country_dict = dict(sorted(country_dict.items()))
    return country_dict



def tokenize(text):

    tokens = []
    if (isinstance(text, str)):
        tokens = text.lower().split()
        return tokens
    if (isinstance(text, list)):
        tokens = []
        for item in text:
            items = item.split()
            for i in items:
                tokens.append(i.lower())
        tokens = set(tokens)

        return list(tokens)


# def stem_tokens(values):
#     ps = nltk.stem.porter.PorterStemmer()
#     stemmed_values = []
#     for value in values:
#         stemmed_values.append(ps.stem(value))
#     return stemmed_values


def search_by_query(dict_terms_posting_list, all_countries):
    print("\nC O U N T R Y   A N A L Y Z E R\n")
    print("You can search for country name, currency, capital, languages, ethnic groups and religion")

    query = input("Search for:").lower()
    while query != 'q':
        # ps = nltk.stem.porter.PorterStemmer()
        # query = ps.stem(query)
        print("searching for ", query)
        docs = []
        try:
            docs = dict_terms_posting_list[query.lower()]
        except:
            print(">> Sorry, we could not find the answer for that")
            query = input("Enter a country:").lower()

        print(docs)
        answer_list = []
        prev_idx = 0
        for idx in docs:
            if prev_idx != idx:
                country = all_countries[idx]
                print(">> Found information about '", query, "':", country)
            prev_idx = idx
        #     if query == country['currency'].lower() or query in country['ethnic_groups'] or query in country['language']:
        #         answer_list.append(country['common_name'])
        # if answer_list:
        #     print(answer_list)
        print("\nYou can search for country name, currency, capital, languages, ethnic groups and religion")
        query = input("Search for:").lower()

def remove_stopwords(values):
    new_values = []
    stopwords = io.open('./root/stopwords.txt', 'r', encoding='utf-8').read()

    for v in values:
        if v + '\n' in stopwords:
            pass
        else:
            new_values.append(v)
    return new_values

def filter_countries(title, text):
    if "history" not in title.lower() and "demographics" not in title.lower() and "geography" not in title.lower():
        if re.findall('Infobox country', text):
            return title, text
        else:
            return None
    else:
        return None

def filter_non_num(dict):
    non_number_fields = ['name', 'capital', 'currency', 'languages', 'ethnic_groups']
    non_number_dict = {x:dict[x] for x in non_number_fields if x in dict}
    return non_number_dict



def main():
    # wiki_file_240MB = './root/enwiki-latest-pages-articles1.xml-p1p41242'
    # wiki_file_350MB = 'C:/Users/vikto/Desktop/FIIT_ING/VINF/enwiki-latest-pages-articles15.xml-p14324603p15824602'
    # wiki_file_390MB = 'C:/Users/vikto/Desktop/FIIT_ING/VINF/enwiki-latest-pages-articles14.xml-p11659683p13159682'
    # wiki_file_480MB = 'C:/Users/vikto/Desktop/FIIT_ING/VINF/enwiki-latest-pages-articles11.xml-p5399367p6899366'
    #
    # # files = {1: '240MB\\1.csv', 2: '240MB\\2.csv', 3: '240MB\\3.csv', 4: '240MB\\4.csv', 5: '240MB\\5.csv', 6: '240MB\\6.csv', 7: '240MB\\7.csv', 8: '240MB\\8.csv', 9: '240MB\\9.csv', 10: '240MB\\10.csv', 11: '240MB\\11.csv', 12: '240MB\\12.csv', 13: '240MB\\13.csv', 14: '240MB\\14.csv', 15: '240MB\\15.csv', 16: '240MB\\16.csv', 17: '240MB\\17.csv', 18: '240MB\\18.csv', 19: '240MB\\19.csv', 20: '240MB\\20.csv', 21: '240MB\\21.csv', 22: '240MB\\22.csv', 23: '240MB\\23.csv', 24: '240MB\\24.csv', 25: '240MB\\25.csv', 26: '240MB\\26.csv', 27: '240MB\\27.csv', 28: '240MB\\28.csv', 29: '240MB\\29.csv', 30: '240MB\\30.csv', 31: '240MB\\31.csv', 32: '240MB\\32.csv', 33: '240MB\\33.csv', 34: '240MB\\34.csv', 35: '240MB\\35.csv', 36: '240MB\\36.csv', 37: '240MB\\37.csv', 38: '240MB\\38.csv', 39: '240MB\\39.csv', 40: '240MB\\40.csv', 41: '240MB\\41.csv', 42: '240MB\\42.csv', 43: '240MB\\43.csv', 44: '240MB\\44.csv', 45: '240MB\\45.csv', 46: '240MB\\46.csv', 47: '240MB\\47.csv', 48: '240MB\\48.csv', 49: '240MB\\49.csv', 50: '240MB\\50.csv', 51: '240MB\\51.csv', 52: '240MB\\52.csv', 53: '240MB\\53.csv', 54: '240MB\\54.csv', 55: '240MB\\55.csv', 56: '240MB\\56.csv', 57: '240MB\\57.csv', 58: '240MB\\58.csv', 59: '240MB\\59.csv', 60: '240MB\\60.csv', 61: '240MB\\61.csv', 62: '240MB\\62.csv', 63: '240MB\\63.csv', 64: '240MB\\64.csv', 65: '240MB\\65.csv', 66: '240MB\\66.csv', 67: '240MB\\67.csv', 68: '240MB\\68.csv', 69: '240MB\\69.csv', 70: '240MB\\70.csv', 71: '240MB\\71.csv', 72: '240MB\\72.csv', 73: '240MB\\73.csv', 74: '240MB\\74.csv', 75: '240MB\\75.csv', 76: '240MB\\76.csv', 77: '240MB\\77.csv', 78: '240MB\\78.csv', 79: '240MB\\79.csv', 80: '240MB\\80.csv', 81: '240MB\\81.csv', 82: '240MB\\82.csv', 83: '240MB\\83.csv', 84: '240MB\\84.csv', 85: '240MB\\85.csv', 86: '240MB\\86.csv', 87: '240MB\\87.csv', 88: '240MB\\88.csv', 89: '240MB\\89.csv', 90: '240MB\\90.csv', 91: '240MB\\91.csv', 92: '240MB\\92.csv', 93: '240MB\\93.csv', 94: '240MB\\94.csv', 95: '240MB\\95.csv', 96: '240MB\\96.csv', 97: '240MB\\97.csv', 98: '240MB\\98.csv', 99: '240MB\\99.csv', 100: '240MB\\100.csv', 101: '240MB\\101.csv', 102: '240MB\\102.csv', 103: '240MB\\103.csv', 104: '240MB\\104.csv', 105: '240MB\\105.csv', 106: '240MB\\106.csv', 107: '240MB\\107.csv', 108: '240MB\\108.csv', 109: '240MB\\109.csv', 110: '240MB\\110.csv', 111: '240MB\\111.csv', 112: '240MB\\112.csv', 113: '240MB\\113.csv', 114: '240MB\\114.csv', 115: '240MB\\115.csv', 116: '240MB\\116.csv', 117: '240MB\\117.csv', 118: '240MB\\118.csv', 119: '240MB\\119.csv', 120: '240MB\\120.csv', 121: '240MB\\121.csv', 122: '240MB\\122.csv', 123: '240MB\\123.csv', 124: '240MB\\124.csv', 125: '240MB\\125.csv', 126: '240MB\\126.csv', 127: '240MB\\127.csv', 128: '240MB\\128.csv', 129: '240MB\\129.csv', 130: '240MB\\130.csv', 131: '240MB\\131.csv', 132: '240MB\\132.csv', 133: '240MB\\133.csv', 134: '240MB\\134.csv', 135: '240MB\\135.csv', 136: '240MB\\136.csv', 137: '240MB\\137.csv', 138: '240MB\\138.csv', 139: '240MB\\139.csv', 140: '240MB\\140.csv', 141: '240MB\\141.csv', 142: '240MB\\142.csv', 143: '240MB\\143.csv', 144: '240MB\\144.csv', 145: '240MB\\145.csv', 146: '240MB\\146.csv', 147: '240MB\\147.csv', 148: '240MB\\148.csv', 149: '240MB\\149.csv', 150: '240MB\\150.csv', 151: '240MB\\151.csv', 152: '240MB\\152.csv', 153: '240MB\\153.csv', 154: '240MB\\154.csv', 155: '240MB\\155.csv', 156: '240MB\\156.csv', 157: '240MB\\157.csv', 158: '240MB\\158.csv', 159: '240MB\\159.csv', 160: '240MB\\160.csv', 161: '240MB\\161.csv', 162: '240MB\\162.csv', 163: '240MB\\163.csv', 164: '240MB\\164.csv', 165: '240MB\\165.csv', 166: '240MB\\166.csv', 167: '240MB\\167.csv', 168: '240MB\\168.csv', 169: '240MB\\169.csv', 170: '240MB\\170.csv', 171: '240MB\\171.csv', 172: '240MB\\172.csv', 173: '240MB\\173.csv', 174: '240MB\\174.csv', 175: '240MB\\175.csv', 176: '240MB\\176.csv', 177: '240MB\\177.csv', 178: '240MB\\178.csv', 179: '240MB\\179.csv', 180: '240MB\\180.csv', 181: '240MB\\181.csv', 182: '240MB\\182.csv', 183: '240MB\\183.csv', 184: '240MB\\184.csv', 185: '240MB\\185.csv', 186: '240MB\\186.csv', 187: '240MB\\187.csv', 188: '240MB\\188.csv', 189: '240MB\\189.csv', 190: '240MB\\190.csv', 191: '240MB\\191.csv', 192: '240MB\\192.csv', 193: '240MB\\193.csv', 194: '240MB\\194.csv', 195: '240MB\\195.csv', 196: '240MB\\196.csv', 197: '240MB\\197.csv', 198: '240MB\\198.csv', 199: '240MB\\199.csv', 200: '240MB\\200.csv', 201: '240MB\\201.csv', 202: '240MB\\202.csv', 203: '240MB\\203.csv', 204: '240MB\\204.csv', 205: '240MB\\205.csv', 206: '240MB\\206.csv', 207: '240MB\\207.csv', 208: '240MB\\208.csv', 209: '240MB\\209.csv'}
    #
    # files = get_country_pages(wiki_file_240MB)
    # # print(files)

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
        .map(lambda row: (row[0], filter_non_num(row[1])))\
        .groupByKey()\
        .mapValues(list)\
        .saveAsTextFile('rdd_selected')

    print("\n ------------------ DF SELECTED ----------------->>\n")
    # print(rdd_selected)
    # for country in rdd_selected:
    #     print(country)
    print("\n <<---------------- DF SELECTED -----------------\n")


    filtered_countries = {}
    all_countries = {}
    # filtered_files = dict(itertools.islice(files.items(), 0, 210))

    # filtered_countries = []
    # for country in rdd_selected:
    #     countries = country[1]
    #     countries['title'] = country[0]
    #     filtered_countries.append(countries)
    #
    # print("\n ------------------ FILTERED ----------------->>\n")
    # print(filtered_countries)
    # print("\n <<---------------- FILTERED -----------------\n")

    dict_terms_posting_list = {}

    # for idx, _file in filtered_files.items():
        # print("id: ", idx)
        # infobox = get_infobox_from_file(_file)
        # country_dict = get_specific_data_from_infobox(_file)
        # country_dict = get_specific_data_from_article(_file)
        # print(idx, country_dict)
    #
    # idx = 0
    # for country in filtered_countries:
    #
    #     non_number_fields = ['name', 'capital', 'currency', 'languages', 'ethnic_groups']
    #     non_number_dict = {x:country[x] for x in non_number_fields if x in country}
    #
    #     all_countries[idx] = country
    #     idx += 1
    #     for k,v in non_number_dict.items():
    #         values = tokenize(v)
    #         # values = stem_tokens(values)
    #         values = remove_stopwords(values)
    #
    #         for value in sorted(values):
    #             dict_terms_posting_list.setdefault(value, []).append(int(idx))
    #
    # dict_terms_posting_list = dict(sorted(dict_terms_posting_list.items()))
    # print("\n\nterms_posting_lists>>>>>", dict_terms_posting_list)
    #
    # search_by_query(dict_terms_posting_list, all_countries)


main()
