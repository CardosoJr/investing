from re import A
from gnews import GNews
from GoogleNews import GoogleNews
import pandas as pd 
from datetime import datetime
from dateutil.relativedelta import relativedelta


class NewsExtractor: 
    def __init__(self):
        self.now = datetime.now()
    
    def extract_history(self, from_date, to_date, filters, max_pages = 10, max_itens = 100, country = "BR", language = "PT", extract_full = False):
        if type(from_date) == str:
            from_date = self.__convert_str_to_date(from_date)

        if type(to_date) == str:
            to_date = self.__convert_str_to_date(to_date)
       
        date_intervals = []
        current = from_date 
        while current < to_date:
            final = current + relativedelta(days = 30)
            if final > to_date:
                final = to_date
            date_intervals.append((current, final))
            current = current + relativedelta(days = 31)

        data = []

        for dt_interval in date_intervals:
            googlenews = GoogleNews(lang = 'pt', start = dt_interval[0].strftime("%d/%m/%Y"), end = dt_interval[1].strftime("%d/%m/%Y"))
            googlenews.search(filters)
            
            num_page = 1
            titles = []

            while len(titles) < max_itens and num_page < max_pages:
                googlenews.get_page(num_page)
                results = googlenews.result()

                df = pd.DataFrame(results)
                df = df[~df['title'].isin(titles)]
                df = df[['title', 'datetime', 'desc', 'link', 'media']]

                df.columns = ['title', 'published date', 'description', 'url', 'publisher']
                titles.extend(df['title'])
                num_page += 1
                data.append(df)
        final_result = pd.DataFrame([])
        if len(data) > 0:
            final_result = pd.concat(data, ignore_index = True)
        
        return final_result


    def __convert_str_to_date(self, date: str):
        return datetime.strptime(date, "%Y-%m-%d")

    def extract_daily(self, from_date, filters, country = "BR", language = "PT", extract_full = False):
        if type(from_date) == str:
            from_date = self.__convert_str_to_date(from_date)

        delta = (self.now - from_date).days

        if delta > 30:
            raise Exception("Period too large. Use Method extract_history instead")
        if delta <= 0:
            delta = 1
            
        period = str(delta) + "d"
        gnews = GNews(language = language, country = country, period = period)
        
        result = gnews.get_news(filters)
        new_result = []
        for x in result:
            x['publisher'] = x['publisher']['title']
            x['title'] = x['title'].replace(" - " + x['publisher'], "")
            new_result.append(x)

        result = new_result

        if extract_full:
            for article in result:
                url = article['url']
                text = gnews.get_full_article(url)
                article['full_text'] = text

        df = pd.DataFrame(result)
        return df
