import datetime as dt
import pandas as pd
import numpy as np
from pathlib import Path
import yaml
import re
import nltk
from sklearn.feature_extraction.text import CountVectorizer
from transformers import AutoTokenizer
from transformers import AutoModelForSequenceClassification
from transformers import MarianTokenizer, MarianMTModel
import numpy as np
from scipy.special import softmax
import csv
import urllib.request
from nltk.tokenize import word_tokenize
import string

"""
Check this out: 
https://github.com/WayneDW/Sentiment-Analysis-in-Event-Driven-Stock-Price-Movement-Prediction
https://github.com/charlesmalafosse/FastText-sentiment-analysis-for-tweets
https://github.com/jbesomi/texthero
https://github.com/Deffro/text-preprocessing-techniques
https://github.com/turian/pytextpreprocess

"""

def RemoveStopWords(instancia):
    stopwords = set(nltk.corpus.stopwords.words('portuguese'))
    palavras = [i for i in instancia.split() if not i in stopwords]
    return (" ".join(palavras))

def Stemming(instancia):
    stemmer = nltk.stem.RSLPStemmer()
    palavras = []
    for w in instancia.split():
        palavras.append(stemmer.stem(w))
    return (" ".join(palavras))


def Limpeza_dados(instancia):
    # remove links, pontos, virgulas,ponto e virgulas dos tweets
    instancia = re.sub(r"http\S+", "", instancia).lower().replace('.','').replace(';','').replace('-','').replace(':','').replace(')','')
    return (instancia)

from nltk.stem import WordNetLemmatizer
wordnet_lemmatizer = WordNetLemmatizer()

def Lemmatization(instancia):
    palavras = []
    for w in instancia.split():
        palavras.append(wordnet_lemmatizer.lemmatize(w))
    return (" ".join(palavras))

def Preprocessing(instancia):
    instancia = re.sub(r"http\S+", "", instancia).lower().replace('.','').replace(';','').replace('-','').replace(':','').replace(')','').replace('"','')
    stopwords = set(nltk.corpus.stopwords.words('portuguese'))
    palavras = [i for i in instancia.split() if not i in stopwords]
    return (" ".join(palavras))

def tokenize(frase):
    return word_tokenize(frase)

def TranslatePt2En(text):
    translation_model_name = f'Helsinki-NLP/opus-mt-roa-en'
    model = MarianMTModel.from_pretrained(translation_model_name)
    tokenizer = MarianTokenizer.from_pretrained(translation_model_name)
    # Translate the text
    inputs = tokenizer(text, return_tensors="pt", padding=True)
    gen = model.generate(**inputs)
    return tokenizer.batch_decode(gen, skip_special_tokens=True)


def cleaning_tweets(tweet, s_ticker):
    whitespace = re.compile(r"\s+")
    web_address = re.compile(r"(?i)http(s):\/\/[a-z0-9.~_\-\/]+")
    ticker = re.compile(fr"(?i)@{s_ticker}(?=\b)")
    user = re.compile(r"(?i)@[a-z0-9_]+")

    tweet = whitespace.sub(" ", tweet)
    tweet = web_address.sub("", tweet)
    tweet = ticker.sub(s_ticker, tweet)
    tweet = user.sub("", tweet)
    return tweet

def clean_text(text):
    whitespace = re.compile(r"\s+")
    user = re.compile(r"(?i)@[a-z0-9_]+")
    user_reddit = re.compile(r"(?i)u/[a-z0-9_]+")

    text = whitespace.sub(" ", text)
    text = user.sub("", text)
    text = user_reddit.sub("", text)
    text = text.replace("\n", " ")
    # text = re.sub('\[.*?\]', '', text)
    # text = re.sub('<.*?>+', '', text)
    text = re.sub(r"https?\S+", "", text)
    text = re.sub(r"&.*?;", "", text)
    text = re.sub(r"<.*?>", "", text)
    # text = re.sub('[%s]' % re.escape(string.punctuation), '', text)
    text = text.replace("RT", "")
    text = text.replace(u"…", "")
    text = text.strip()
    return text

def clean_text_sentiment(text):
    # clean up text for sentiment analysis
    text = re.sub(r"[#|@]\S+", "", text)
    text = text.strip()
    return text

def preprocess(text):
    new_text = []
    for t in text.split(" "):
        t = '@user' if t.startswith('@') and len(t) > 1 else t
        t = 'http' if t.startswith('http') else t
        new_text.append(t)
    return " ".join(new_text)

def PredictSentimentScores(text):
    task='sentiment'
    MODEL = f"cardiffnlp/twitter-roberta-base-{task}"
    tokenizer = AutoTokenizer.from_pretrained(MODEL)
    model = AutoModelForSequenceClassification.from_pretrained(MODEL)
    encoded_input = tokenizer(text, return_tensors='pt')
    output = model(**encoded_input)
    scores = output[0].detach().numpy()
    scores = softmax(scores, axis = 1)
    return scores # Negative, neutral, positive
    
def ProcessTextPipeline(text):
    translations = TranslatePt2En(text)
    scores = PredictSentimentScores(translations)
    return scores


class SentimentPipeline:
    def __init__(self):
        translation_model_name = f'Helsinki-NLP/opus-mt-roa-en'
        self.trl_model = MarianMTModel.from_pretrained(translation_model_name)
        self.trl_tokenizer = MarianTokenizer.from_pretrained(translation_model_name)

        task='sentiment'
        MODEL = f"cardiffnlp/twitter-roberta-base-{task}"
        self.sent_tokenizer = AutoTokenizer.from_pretrained(MODEL)
        self.sent_model = AutoModelForSequenceClassification.from_pretrained(MODEL)

    def translate(self, text):
        inputs = self.trl_tokenizer(text, return_tensors="pt", padding=True)
        gen = self.trl_model.generate(**inputs)
        return self.trl_tokenizer.batch_decode(gen, skip_special_tokens=True)

    def calculate_sentiments(self, text):
        encoded_input = self.sent_tokenizer(text, return_tensors='pt', padding = True)
        output = self.sent_model(**encoded_input)
        scores = output[0].detach().numpy()
        scores = softmax(scores, axis = 1)
        return scores

    def process(self, text):
        return self.calculate_sentiments(self.translate(text))

