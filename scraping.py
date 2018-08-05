import pandas as pd
from bs4 import SoupStrainer

from helpers import *

URL_FOND = "https://www.nordnet.no/mux/web/fonder/sok.html?nn_kategori=&kategori=&forvaltare=&sokord=&sok=1&ppm=0&nobuy=&flik=&nm=&typ=1"

URL_DETAIL_PAGE_PREFIX = 'https://secust.msse.se/se/nordnetny/funds/' 

def create_dataframe_in_parallel(urls, pool_size):
    
    result = pd.DataFrame()
    
    from multiprocessing import Pool
    with Pool(pool_size) as p:
        dataframes = p.map(create_fund_dataframe, urls)

    for dataframe in dataframes:
        result = result.append(dataframe)
        
    return result


def create_fund_dataframe(url):
    only_iframe = SoupStrainer("iframe")
    detail_page_soup = get_soup_with_strainer(url, only_iframe)

    url_postfix = detail_page_soup.find(lambda tag: tag.name == "iframe").get('src').split("overview")[1]

    url_portfolio_page = URL_DETAIL_PAGE_PREFIX + 'portfolio' + url_postfix
    url_overview_page = URL_DETAIL_PAGE_PREFIX + 'overview' + url_postfix

    only_tables = SoupStrainer("table") 
    portefolio_soup = get_soup_with_strainer(url_portfolio_page, only_tables)    
    overview_soup = get_soup_with_strainer(url_overview_page, only_tables)

    fund_dataframe = get_overview_dataframe(overview_soup)
    fund_dataframe['Name'] = get_fund_name_from_url(url)

    return fund_dataframe


def get_overview_dataframe(overview_soup):
    
    dataFrame_costs = create_cost_dataframe(overview_soup)
    dataFrame_volatility =  create_volatility_dataframe(overview_soup)

    return pd.concat([dataFrame_costs, dataFrame_volatility], axis=1)


def create_cost_dataframe(soup):
    
    table_soup = soup.find("td", text = "Kjøp").parent.parent
    
    row_names = ["Kjøp", "Selg", "Forvaltningsavgift", "Resultatbasert avgift(maks)", "Løpende kostnader"]
    td_tags = table_soup.findAll("td", class_=None)

    values = get_floats_as_strings_from_tags(td_tags)
    columns = ['Kjøpsavgift', "Salgsavgift", "Forvaltningsavgift", "Resultatbastert avgift(maks)", "Løpende kostnader"]    
    
    return create_df(columns, values)

def create_volatility_dataframe(soup):
    
    table_soup = soup.find("td", text = "Gj.snitt avkastning").parent.parent

    row_names = ["Gj.snitt avkastning", "Standardavvik", "Sharpe Ratio"]
    td_tags = table_soup.findAll("td", class_=None)

    values = get_floats_as_strings_from_tags(td_tags)
    columns = ["Gj.snitt avkastning", "Standardavvik", "Sharpe Ratio"]

    return create_df(columns, values)

