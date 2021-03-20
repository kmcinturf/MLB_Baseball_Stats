# PitcherScrape.py
#Created 3/18/2021: Efrain Lopez, Kevin McInturf, Michael Regpala
#Description: Uses splinter to scrape pitcher information from fantasypros.com
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
url = 'https://www.fantasypros.com/mlb/rankings/sp.php/'
browser.visit(url)

#The following input statement is used to slow down the program to give user a chance to accept cookie as well as limit players.
TopN = int(input("Please go to open Chrome Browser and Accept Cookies. Also enter number of players to extract by rank (Top N)"))

#Use Pandas to import HTML table with player ranking information.  Ranking HTML table is first table in list
pitching = pd.read_html(browser.html)

#Ranking HTML table is first DataFrame on LIst
pitching_rank_df = pitching[0]

#Scrape/Parse Initial page to build list of href addresses to scrape.
bs = BeautifulSoup(browser.html, 'html.parser')
results = bs.find_all('a',class_="player-name")
aref_list = []
for result in results:
    aref_list.append(result['href'])

#Utilize list of href to scrape player bio and stats pages and store into a list of DataFrames.
bs = BeautifulSoup(browser.html, 'html.parser')
list_df_pitchers = []
list_df_pitcher_stats = []

for aref in aref_list[:TopN]:
    bio_dict = {}
    time.sleep(.5)
    browser.click_link_by_href(aref)   
    #browser.links.find_by_href(aref) 
    time.sleep(.5)
    bio = BeautifulSoup(browser.html,'html.parser')

    #Calculate player name
    bio_name = bio.find('div','pull-left primary-heading-subheading')
    player = bio_name.text.lstrip().split('\n')[0].rstrip()
    print(player,flush=True)
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

    #Append Player Bio ijnformation into list of dataframe
    bio_dict = {"PLAYER":player,"AGE":age, "COLLEGE":college, "BATS":bats,"THROWS":throws}
    bio_df = pd.DataFrame([bio_dict])
    list_df_pitchers.append(bio_df)
    
    #CLick thru to player stat page
    browser.click_link_by_href('/mlb/stats/' + aref.split('/')[3].split('?')[0])
    time.sleep(.5)
    stats_df = pd.read_html(browser.html)[0]
    cols = stats_df.columns.droplevel(0)
    stats_df.set_axis(cols,axis='columns',inplace=True)
    #Shoehi Oethani is a DH and Pitcher had to HTML tables.
    if 'AB' in stats_df.columns:
        stats_df=pd.read_html(browser.html)[1]
        cols = stats_df.columns.droplevel(0)
        stats_df.set_axis(cols,axis='columns',inplace=True)
    stats_df["PLAYER"] = player
    list_df_pitcher_stats.append(stats_df)
    time.sleep(.5)
    browser.back()
    browser.back()

#Transform Player Ranking Data
cols = ['PLAYER_RANK','PLAYERPOS','OVERALL','BEST','WORST','RANK_AVG','STD_DEV','ADP','VS_ADP','NOTES']
pitching_rank_df.set_axis(cols,axis='columns',inplace=True)
#Create PLAYER column in DataFrame. This will be common player ID included in all related tables.
player_series = pitching_rank_df['PLAYERPOS']
player_list = []
for player in player_series:
    player_list.append(player.split('(')[0].rstrip())
pitching_rank_df['PLAYER'] = player_list
pitching_rank_df["OVERALL"].fillna(0, inplace = True)
pitching_rank_df = pitching_rank_df.astype({"OVERALL": int})
pitching_rank_df = pitching_rank_df.set_index("PLAYER_RANK")

#Transform Player Stats Data
for pitcher in list_df_pitcher_stats:
    cols = ["SEASON","TEAM","GAMES","GS","W","L","SV","BS","HD","CG","SHO","IP","H","R","ER",
            "HR","BB","BB_PCT","SO","K_PCT","ERA","WHIP","PLAYER"]
    pitcher.set_axis(cols,axis='columns',inplace=True)
    pitcher.loc[:,"SEASON"] = pitcher.loc[:,"SEASON"].ffill()
    pitcher["TEAM"].fillna("", inplace = True)
    pitcher.set_index("SEASON", inplace=True)
    pitcher.drop(index="Totals", inplace=True)

#Connect to Postgres Database
engine = create_engine(f'postgresql://{username}:{password}@localhost:5432/{databasename}')
conn = engine.connect()

#Truncate and Load  Postgres tables
engine.execute('TRUNCATE TABLE public."PITCHER_RANKINGS"')
pitching_rank_df.to_sql('PITCHER_RANKINGS', con = engine, if_exists= 'append', index = True)

engine.execute('TRUNCATE TABLE public."PITCHER_BIO"')
for pitcher in list_df_pitchers:
    df_temp = pitcher.set_index('PLAYER')
    df_temp.to_sql('PITCHER_BIO', con = engine, if_exists= 'append', index = True)

engine.execute('TRUNCATE TABLE public."PITCHER_STATS"')
for pitcher in list_df_pitcher_stats:
    pitcher.to_sql('PITCHER_STATS', con = engine, if_exists= 'append', index = True)

print("***************Success*****************")
print(f"{TopN} Players Processed")