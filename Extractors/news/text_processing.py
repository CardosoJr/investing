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
    text = [preprocess(x) for x in text]
    encoded_input = tokenizer(text, return_tensors='pt')
    output = model(**encoded_input)
    scores = output[0].detach().numpy()
    scores = softmax(scores, axis = 1)
    return scores # Negative, neutral, positive
    
def ProcessTextPipeline(text):
    translations = TranslatePt2En(text)
    scores = PredictSentimentScores(translations)
    return scores