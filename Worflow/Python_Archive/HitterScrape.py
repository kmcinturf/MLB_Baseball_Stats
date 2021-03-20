# HitterScrape.py
#Created 3/18/2021: Efrain Lopez, Kevin McInturf, Michael Regpala
#Description: Uses splinter to scrape hitter information from fantasypros.com
#             Beautiful Soup is used to scrape player bio and links.
#             Use SQL Alchemy to load Pandas DataFrames into Postgres table.


#Import Python Modules
from splinter import Browser
from bs4 import BeautifulSoup
from webdriver_manager.chrome import ChromeDriverManager
import pandas as pd
import time 
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

#Set database connection variables where target tables exists. 
# For more information on table creation see README file regarding running DDL script.
username = "postgres"
password = "password"
databasename = "MLB"

# Setup splinter browser driver
executable_path = {'executable_path': ChromeDriverManager().install()}
browser = Browser('chrome', **executable_path, headless=False)

#Go to FantasyPros hitter ranking page
url = 'https://www.fantasypros.com/mlb/rankings/hitters.php'
browser.visit(url)

#The following input statement is used to slow down the program to give user a chance to accept cookie as well as limit players.
TopN = int(input("Please go to open Chrome Browser and Accept Cookies. Also enter number of players to extract by rank (Top N)"))

#Use Pandas to import HTML table with player ranking information.  Ranking HTML table is first table in list
hitters = pd.read_html(browser.html)

#Ranking HTML table is first DataFrame on LIst
hitters_rank_df = hitters[0]

#Parse home page to gather links to traverse   
bs = BeautifulSoup(browser.html, 'html.parser')
results = bs.find_all('a',class_="player-name")
aref_list = []
for result in results:
    aref_list.append(result['href'])

#Utilize list of href to scrape player bio and stats pages and store into a list of DataFrames.
bs = BeautifulSoup(browser.html, 'html.parser')
list_df_hitter = []
list_df_hitter_stats = []
for aref in aref_list[:TopN]:
    bio_dict = {}
    time.sleep(1)
    browser.click_link_by_href(aref)
    time.sleep(1)
    bio = BeautifulSoup(browser.html,'html.parser')

    #Calculate player name
    bio_name = bio.find('div','pull-left primary-heading-subheading')
    player = bio_name.text.lstrip().split('\n')[0].rstrip()
    print(player, flush=True)
    bio_results = bio.find_all('span','bio-detail')
    #Get Player Bio information
    college = ''
    for bio_result in bio_results:
        attr = bio_result.text.split(':')[0]
        if (attr == "Age"):
            age = int(bio_result.text.split(':')[1])
        elif (attr == "College"):
            college = bio_result.text.split(':')[1].lstrip()
        elif (attr == "Bats" ):
            bats = bio_result.text.split(':')[1].lstrip()
        elif (attr == "Throws"):
            throws = bio_result.text.split(':')[1].lstrip()
            
    #Append Player Bio information into list of dataframe
    bio_dict = {"PLAYER":player,"AGE":age, "COLLEGE":college, "BATS":bats,"THROWS":throws}
    bio_df = pd.DataFrame([bio_dict])
    list_df_hitter.append(bio_df)
    
    #CLick thru to player stat page
    browser.click_link_by_href('/mlb/stats/' + aref.split('/')[3].split('?')[0])
    time.sleep(1)
    stats_df = pd.read_html(browser.html)[0]
    #Transform Player Bio information during extract. Clean up table header, Multi Index. Set DataFrame INdex
    cols = stats_df.columns.droplevel(0)
    stats_df.set_axis(cols,axis='columns',inplace=True)
    stats_df["PLAYER"] = player
    #Append Player Stats information to list of DataFrames
    list_df_hitter_stats.append(stats_df)
    time.sleep(1)
    browser.back()
    time.sleep(1)
    browser.back()


#Tranform/Cleanup Player Ranking DataFrame
#Clean up column names in DataFrame
cols = ['PLAYER_RANK','PLAYERPOS','OVERALL','BEST','WORST','RANK_AVG','STD_DEV','ADP','VS_ADP','NOTES']
hitters_rank_df.set_axis(cols,axis='columns',inplace=True)
#Create PLAYER column in DataFrame. This will be common player ID included in all related tables.
player_series = hitters_rank_df['PLAYERPOS']
player_list = []
for player in player_series:
    player_list.append(player.split('(')[0].rstrip())
hitters_rank_df['PLAYER'] = player_series
hitters_rank_df["OVERALL"].fillna(0, inplace = True)
hitters_rank_df = hitters_rank_df.astype({"OVERALL": int})
hitter_rankings = hitters_rank_df.set_index("PLAYER_RANK")





#Transform/Cleanup Player Stats
for hitter in list_df_hitter_stats:
    cols = ["SEASON","TEAM","GAMES","AB","R","H","SECOND_BASE",
            "THIRD_BASE","HR","RBI","BB","HBP","SF","SO","SB",
            "CS","BATTING_AVG","OBP","SLG","OPS","BABIP","PLAYER"]
    hitter.set_axis(cols,axis='columns',inplace=True)
    hitter.loc[:,"SEASON"] = hitter.loc[:,"SEASON"].ffill()
    hitter["TEAM"].fillna("", inplace = True)
    hitter.set_index("SEASON", inplace=True)
    hitter.drop(index="Totals", inplace=True)



#Connect to Postgres Database
engine = create_engine(f'postgresql://{username}:{password}@localhost:5432/{databasename}')
conn = engine.connect()

#Truncate and  Load Corresponding Postgres tables
# Truncate table before inserting
engine.execute('TRUNCATE TABLE public."HITTER_RANKINGS"')
hitter_rankings.to_sql('HITTER_RANKINGS', con = engine, if_exists = 'append', index = True )

engine.execute('TRUNCATE TABLE public."HITTER_BIO"')
for hitter in list_df_hitter:
    df_temp = hitter.set_index('PLAYER')
    df_temp.to_sql('HITTER_BIO', con = engine, if_exists= 'append', index = True)

engine.execute('TRUNCATE TABLE public."HITTER_STATS"')
for hitter in list_df_hitter_stats:
    hitter.to_sql('HITTER_STATS', con = engine, if_exists= 'append', index = True)
    
print("***************Success*****************")
print(f"{TopN} Players Processed")