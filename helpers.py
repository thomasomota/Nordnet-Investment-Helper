from bs4 import BeautifulSoup
import pandas as pd
import requests
import re

def get_soup(url):
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'lxml') 
    return soup

def get_soup_with_strainer(url, strainer):
    response = requests.get(url)
    return BeautifulSoup(response.text, 'lxml', parse_only=strainer) 

def get_urls(soup, **kwargs):
    
    if 'regex' in kwargs:
        regex_string = kwargs.get('regex')
        tags = soup.find_all('a', attrs={'href': re.compile(regex_string)})
    else:
        tags = soup.find_all('a')
    
    urls = []  
    prefix = "" if 'prefix' not in kwargs else kwargs.get('prefix')
    result = [prefix + tag.get('href') if prefix else tag.get('href') for tag in tags]

    return result

def get_floats_as_strings_from_tags(tags):
    return [re.findall("\d+\.\d+", tag.text)[0] if re.findall("\d+\.\d+", tag.text) else '0' for tag in tags]
    
def get_fund_name_from_url(url):
    return url.split('nm=')[-1].replace("+", " ")

    
def create_df(columns, values):
    
    data = {}
    for x in range(0, len(columns)):
        name = columns[x]
        value = values[x]
        data[name] = value

    return pd.DataFrame(data, columns= columns, index=[0])



