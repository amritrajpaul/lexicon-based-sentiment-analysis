import pandas as pd
import codecs
from nltk.tokenize import word_tokenize
#from sklearn.metrics import accuracy_score
#from sklearn.metrics import f1_score
import requests
from bs4 import BeautifulSoup 
import pandas as pd
from selenium import webdriver  #to handle news source's dynamic website
import datetime
import time
from statistics import mean
import quandl
import sys
import xlwt 
import openpyxl
import re

stime = time.time()


def RB_Sentiment_Analyser(text_df):
    print("Sentiment Analyser Initialized links with keyword --- %s seconds ---" % (time.time() - stime))
    #here we read the HSWN text file
    data = pd.read_csv("HindiSentiWordnet.txt", delimiter=' ')
    fields = ['POS_TAG', 'ID', 'POS', 'NEG', 'LIST_OF_WORDS']
    #Creating a dictionary which contain a tuple for every word. Tuple contains a list of synonyms,
    # positive score and negative score for that word.
    words_dict = {}
    for i in data.index:
        #print(data[fields[0]][i], data[fields[1]][i], data[fields[2]][i], data[fields[3]][i], data[fields[4]][i])
        words = data[fields[4]][i].split(',')
        for word in words:
            words_dict[word] = (data[fields[0]][i], data[fields[2]][i], data[fields[3]][i])
    def sentiment(text):
        words = word_tokenize(text)
        votes = []
        pos_polarity = 0
        neg_polarity = 0
        #adverbs, nouns, adjective, verb are only used
        allowed_words = ['a','v','r','n']
        for word in words:
            if word in words_dict:
                #if word in dictionary, it picks up the positive and negative score of the word
                pos_tag, pos, neg = words_dict[word]
                # print(word, pos_tag, pos, neg)
                if pos_tag in allowed_words:
                    if pos > neg:
                        pos_polarity += pos 
                        votes.append(1)
                    elif neg > pos:
                        neg_polarity += neg
                        votes.append(0)
        #calculating the no. of positive and negative words in total in a review to give class labels
        pos_votes = votes.count(1)
        neg_votes = votes.count(0)
        if pos_votes > neg_votes:
            return 1
        elif neg_votes > pos_votes:
            return 0
        else:
            if pos_polarity < neg_polarity:
                return 0
            else:
                return 1
    dates = []
    art_senti_scores = []
    for i, row in enumerate(text_df.values):
        para_senti_score = []
        date = text_df.index[i]
        hl,art = row
        for para in art:
            para_senti_score.append(sentiment(para))
            #time.sleep(2)
        art_senti_scores.append(mean(para_senti_score))
        dates.append(date)
        sys.stdout.write('\rSentiment Analysed of article : {}/{} ...{} sec'.format(i+1,len(text_df),(time.time() - stime)))
        sys.stdout.flush()
    text_df["Senti Score"] = art_senti_scores
    data = quandl.get("BSE/SENSEX", authtoken="1SnsWfT7hPSiUcZumsa1",start_date = text_df.index.date[-1],end_date = text_df.index.date[0])
    data['sensex_open_to_close_price'] = ((data['Close'] - data['Open'])/data['Open'] )*100
    text_df.to_excel('RB_SentimentScoreForNifty&Sensex.xlsx', sheet_name='Sheet1', index=True, encoding=None)
    data.to_excel('Sensex_data_RB.xlsx', sheet_name='Sheet1', index=True, encoding=None)
    print("2 : xls file is successfully created! named : SentimentScoreForNifty&Sensex.xls , Sensex_data.xls")
    print(text_df)

def parse_article(links,saved_links_title):

    '''This function opens individual relevant article through the link provided from the parse() function below 
    and uses beautiful soup library to extract the article content and their published dates'''
    print("Begun extracting each article from fitered links --- %s seconds ---" % (time.time() - stime))
    saved_articles = []
    saved_article_dates =[]
    for link in links:#saved_requestable_links:
        article = []
        article_content = requests.get(link).content
        article_soup = BeautifulSoup(article_content,'html.parser')
        paras = article_soup.findAll("p",{'style':"word-break:break-word"})
        dateandtime = article_soup.find("meta", {"property": "article:published_time"}).attrs['content']
        dateandtime = dateandtime[:-6]
        for para in paras:
            #article = ''.join(para.get_text())
            article.append(para.get_text())
        saved_articles.append(article)
        date_time_obj = datetime.datetime.strptime(dateandtime, '%Y-%m-%dT%H:%M:%S')
        saved_article_dates.append(date_time_obj)
    dic = {'Headlines':saved_links_title,'Articles':saved_articles}
    hin_df = pd.DataFrame(dic,index = saved_article_dates)
    print("Done! --- %s seconds ---" % (time.time() - stime))
    RB_Sentiment_Analyser(hin_df)

def parse(keywords):

    '''This function opens the website scrolls down for 100 seconds then takes the page source code 
    to traverse and extract news Headlines and Executable Links of relevant articles using keywords,
    Then calls the above function parse_article() with executable link as a parameter'''
    home_link = 'https://www.bhaskar.com/business/'
    print("Begun Parsing and filtering links with keyword --- %s seconds ---" % (time.time() - stime))
    driver = webdriver.Chrome('C:\Program Files\Google\Chrome\Application\chromedriver')
    #url = 'https://www.bhaskar.com/business/'
    driver.get(home_link)
    time.sleep(10)
    prev_height = driver.execute_script('return document.body.scrollHeight;')
    limit = 0
    hours = input('Enter Hour : ')
    minutes = input('Enter Minutes : ')
    secs = input('Enter seconds :')
    seconds = (int(hours)*60 + int(minutes))*60 + int(secs)
    iterations = seconds/4
    while limit < iterations: #6000 = 6.5 hours ,Increase this limit for scraping more article
        driver.execute_script('window.scrollTo(0, document.body.scrollHeight);')
        time.sleep(4)
        new_height = driver.execute_script('return document.body.scrollHeight;')
        #if new_height == prev_height:
        #    break
        prev_height = new_height
        limit += 1
    markup = driver.page_source
    soup = BeautifulSoup(markup,'html.parser')
    links = driver.execute_script
    links = soup.findAll("li",{"class" : '_24e83f49 e54ee612'})
    saved_links = []
    saved_links_title =[]
    saved_requestable_links = []
    for link in links:
        for keyword in keywords:
            if keyword in link.text:
                if link not in saved_links: #this condition stops duplicate links
                    saved_links.append(link)
                    saved_links_title.append(link.text)
                    saved_requestable_links.append(str(home_link) + str(link('a')[0]['href']))
    print("Done! --- %s seconds ---" % (time.time() - stime))
    print('{} articles to be passed for scraping'.format(len(saved_requestable_links)))
    parse_article(saved_requestable_links,saved_links_title)

parse(['सेंसेक्स'])
