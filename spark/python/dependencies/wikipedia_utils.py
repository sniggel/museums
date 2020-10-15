import re

import requests
from bs4 import BeautifulSoup


def fetch_museums_stats(url):
    vcard_class_names = ['infobox', 'geography', 'vcard']
    res = requests.get(url)
    html = res.text
    soup = BeautifulSoup(html, 'html.parser')
    data = []
    info_vcards = soup.find_all('table')
    match_and_value = {}
    for i in info_vcards:
        common_elements = [x for x in vcard_class_names if x in i.attrs.get('class', 'n/a')]
        match_and_value[len(common_elements)] = i

    maximum = sorted(match_and_value.keys())[-1]
    table_body = match_and_value[maximum].find('tbody')

    rows = table_body.find_all('tr')
    for row in rows:
        if row.find_all('th') and row.find_all('td'):
            cols = row.find_all(['th', 'td'])
            cols = [ele.text.strip() for ele in cols]
            data.append([cleanup_column_data(ele, skip_digits=True) for ele in cols if ele]) # Get rid of empty values

    return data


def fetch_city_population(url):
    vcard_class_names = ['infobox', 'geography', 'vcard']
    res = requests.get(url)
    html = res.text
    soup = BeautifulSoup(html, 'html.parser')
    population = 'und'
    info_vcards = soup.find_all('table')
    match_and_value = {}
    for i in info_vcards:
        common_elements = [x for x in vcard_class_names if x in i.attrs.get('class', 'n/a')]
        match_and_value[len(common_elements)] = i

    maximum = sorted(match_and_value.keys())[-1]
    table_body = match_and_value[maximum].find('tbody')

    rows = table_body.find_all('tr')
    for row in rows:
        th_elements = row.find_all('th')
        for t in th_elements:
            if 'Population' in t.text:
                population = t.find_next('td').text
    return cleanup_number(population)


def read_main_table(url, class_name, column_mapper):
    data = list()
    res = requests.get(url)
    html = res.text

    soup = BeautifulSoup(html, 'html.parser')
    table = soup.find('table', attrs={'class': class_name})

    table_body = table.find('tbody')
    rows = table_body.find_all('tr')

    for row in rows:
        cols = row.find_all('td')
        if cols:
            columns_cleaned = column_mapper(cols)
            if len(columns_cleaned) > 0:
                data.append(columns_cleaned)
    return data


def read_largest_cities_table(url='https://en.wikipedia.org/wiki/List_of_largest_cities',
                              class_name='sortable wikitable mw-datatable'):
    largest_cities = read_main_table(url, class_name, map_largest_cities_columns)
    return largest_cities


def read_museums_table(url='https://en.wikipedia.org/wiki/List_of_most-visited_museums',
                       class_name='wikitable sortable'):
    museums = read_main_table(url, class_name, map_museum_columns)
    return museums


def map_largest_cities_columns(columns):
    columns_cleaned = list()
    # city_name
    columns_cleaned.append(cleanup_column_data(columns[0].text.strip()) or 'n/a')
    # city_population
    columns_cleaned.append(cleanup_column_data(columns[5].text.strip()) or 0)
    return columns_cleaned


def map_museum_columns(columns):
    columns_cleaned = list()
    # museum_name
    columns_cleaned.append(cleanup_column_data(columns[0].text.strip()))
    # museum_href
    columns_cleaned.append(f"https://en.wikipedia.org{columns[0].find('a').get('href')}")
    # museum_city
    columns_cleaned.append(cleanup_column_data(columns[1].text.strip()))
    # museum_city_href
    columns_cleaned.append(f"https://en.wikipedia.org{columns[1].find_all('a')[1].get('href')}")
    # museum_visitors_per_year
    columns_cleaned.append(cleanup_column_data(columns[2].text.strip()))
    # year_reported
    columns_cleaned.append(cleanup_column_data(columns[3].text.strip()))
    return columns_cleaned


def cleanup_column_data(data, skip_digits=False):
    if not skip_digits and len(data) > 0 and data[0].isdigit():
        return cleanup_number(data)
    else:
        return data.replace('[c]', '').replace('[d]', '').replace('[zh]', '').replace(u'\xa0', u'')\
            .replace('\ufeff', '').replace('N/A', '')


def cleanup_number(string) -> int:
    if ' million' in string:
        return int(string.replace(' million', '').split('.')[0]) * 1000000
    no_spaces = string.strip()
    no_commas = no_spaces.replace(',', '')
    no_citations = re.sub(r'\[\w{1,20}\]', '', no_commas)
    no_year = re.sub(r'\(\d{4}\)', '', no_citations)
    no_ranks = re.sub(r'\(\d{1,20}\w{1,20}\)', '', no_year).split(' ')[0]
    no_na = no_ranks.replace('N/A', '')
    return int(no_na)
