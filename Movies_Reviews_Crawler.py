# -*- coding: utf-8 -*-
"""
Created on Tue Sep 18 16:57:08 2018

@author: Bing
"""

import re
import time
import requests
import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
import matplotlib.pyplot as plt
import progressbar

class MoviesReviews_Crawler:
    def __init__(self, homepage_url = 'https://movies.yahoo.com.tw/chart.html?cate=year'):
        self.__homepage_url = homepage_url
    
    def __homepage_scraping(self, url):
        res = requests.get(url = url)
        doc = BeautifulSoup(res.text, 'lxml')
        
        # get names of each movies
        first_movie = [name.text for name in doc.select(selector = 'h2')][0]
        movie_names = [name.text for name in doc.select(selector = '.rank_txt')]
        movie_names.insert(0, first_movie)
        
        # get the hyperlinks of each movies
        rank_table = doc.find_all(name = 'div', attrs = 'rank_list table rankstyle1')[0]
        link_list = [link['href'] for link in rank_table.find_all(name = 'a')[::2]]
        
        self.__link = link_list
        
        # get score of each movies
        score = [float(star['data-num']) for star in rank_table.find_all('h6')]
        # get releasing time of each movies
        tags = rank_table.find_all('div', attrs = 'td')
        content = [i.text for i in tags]
        date_pattern = re.compile(pattern = '[0-9]{4}-[0-9]{2}-[0-9]{2}')
        
        date_list = list()
        for element in content:
            mat = date_pattern.match(string = element)
            if mat != None:
                date_list.append(mat.group())
        movies_info = {'names': movie_names, 'link': link_list, 'score': score, 'date': date_list}
        df = pd.DataFrame(data = movies_info, columns = ['names', 'score', 'date', 'link'])
        return df
    
    def __extend_page(self, url):
        res = requests.get(url = url)
        doc = BeautifulSoup(res.text, 'lxml')
        
        href_pattern = re.compile(pattern = r'href=.*\"')
        user_comment = doc.find_all('div', 'btn_plus_more usercom_more gabtn')[0]
        string = str(user_comment.find_all('a')[0])
        extend_url = href_pattern.findall(string)[0][6:][:-1]
        
        return extend_url
    
    def __get_total_pages(self, extend_url):
        res = requests.get(url = extend_url)
        doc = BeautifulSoup(res.text, 'lxml')
        page_numbox = doc.find_all(name = 'div', attrs = "page_numbox")[0]
        total_pages = int(page_numbox.find_all('a')[-2].text)
        
        return total_pages
    
    def __user_comment(self, user_comment_url):
        res = requests.get(url = user_comment_url)
        doc = BeautifulSoup(res.text, 'lxml')
        # get user names 
        user = [user.text[4:] for user in doc.find_all(name = 'div', attrs = 'user_id unuser')]
        # get user publish_time
        publish_time = [time.text[5:] for time in doc.find_all(name = 'div', attrs = 'user_time unuser')]
        # get user comment
        pattern = re.compile(pattern = r'\n|\r')
        form_list = doc.find_all(name = 'form', attrs = 'form_good')
        comment_list = list()
        for form in form_list:
            comment = [comment.text for comment in form.find_all('span')]
            comment = ''.join(comment)
            clear_comment = pattern.sub(repl = '', string = comment)
            comment_list.append(clear_comment)
            
        comment_info = {'user_name': user, 'publish_time': publish_time, 'comment': comment_list}
        
        return comment_info
    
    def __dict_to_df(self, comment_dict):
        count = 0
        for key in comment_dict:
            temp_df = pd.DataFrame(data = comment_dict[key], columns = ['user_name', 'publish_time', 'comment'])
            if count == 0:
                all_comment_df = temp_df
            else:
                all_comment_df = pd.concat([all_comment_df, temp_df])
            count += 1
        return all_comment_df
          
    def get_data(self):
        df = self.__homepage_scraping(url = self.__homepage_url)
        nrow = df.shape[0]
        pbar = progressbar.ProgressBar().start() # adding progressbar
        
        count = -1
        comment_dict = {}
        for link in self.__link:
            count += 1
            extend_url = self.__extend_page(url = link)
            total_pages = self.__get_total_pages(extend_url = extend_url)
            all_comment_url = [extend_url + '?sort=update_ts&order=desc&page=' + str(num) for num in range(1, total_pages + 1)]
            
            count_comment = -1
            for comment_url in all_comment_url:
                count_comment += 1
                #print('The {} comment page of the {} movies.'.format(count_comment + 1, count + 1))
                temp_comment = self.__user_comment(user_comment_url = comment_url)
                time.sleep(np.random.uniform(low = 1, high = 2))
                # combine two dictionary by the same key
                if count_comment == 0:
                    comment_info = temp_comment
                else:
                    for k in comment_info:
                        comment_info[k].extend(temp_comment[k]) # update the content of list in dictionary by key
            
            pbar.update(((count + 1)/nrow)*100)            
            comment_dict.update({count:comment_info})
            
        self.df = df
        self.comment_df = self.__dict_to_df(comment_dict) # combine all dictionary to DataFrame
        
        pbar.finish()
        
        return 'The Web Crawler had finished.'
    
    def output_csv(self, main_table_path, main_table_name, comment_folder_path, comment_file_name):
        
        main_table_file_path = main_table_path + '\\' + main_table_name + '.csv'
        self.df.to_csv(main_table_file_path, encoding = 'UTF-8')
        
        comment_file_path = comment_folder_path + '\\' + comment_file_name + '.csv'
        self.comment_df.to_csv(comment_file_path, encoding = 'UTF-8')
        
if __name__ == '__main__':
    movie = MoviesReviews_Crawler()
    movie.get_data()
    movie.output_csv(main_table_path = 'Movies data', main_table_name = 'main_table', 
                  comment_folder_path = 'Movies data', comment_file_name = 'all_comments')