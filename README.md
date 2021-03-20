# Fantasy Baseball League

In this project we have an upcoming fantasy baseball draft. Unfortunately, for the past couple of years we have been coming up short in our pursuit of the championship. However, this year we plan on using our knowledge of web scraping and data integration to gain a competitive edge over our competition. We will gain an upper hand in this year's Fantasy Baseball League by building a database filled with player stats from Major League Baseball's (MLB).

![Fantasy Baseball](Images/Fantasy_Baseball.jpg)

## Set Up

We have to different two different working scripts for starting pitchers and hitters. We used Jupyter Notebook as a starting point in order run code interactively for debugging purposes and developing proof of concept. Afterwards we converted our Jupyter Notebooks to Python script allowing it to run from the command line. Prior to running our Jupyter Notebook or Python file you must first run our [DBL](DBL.sql). 

![ERD](Images/ERD.jpg)

## Data Cleanup & Analysis

* We will gather our data for the MLB's highest ranked starting pitchers. In order to create our database we will be using Splinter, BeautifulSoup, and ChromeDriverManager to automate the data extraction process from [Fantasy Pros](https://www.fantasypros.com/mlb/rankings/sp.php). These tools will allow us to extract the following data for our starting pitchers:

    * Player rankings including their worst, best, and current rankings as well as their average draft position. 

    * Player bio including name, college, age, and they dominant hand when batting and throwing.

    * Players individual stats starting with the 2014 season and ending with the most recent season of 2020. The exception to this rule are players who haven't been in the league since 2014.

* We will gather our data for the MLB's highest ranked hitters. In order to create our database we will be using Splinter, BeautifulSoup, and ChromeDriverManager to automate the data extraction process from [Fantasy Pros](https://www.fantasypros.com/mlb/rankings/hitters.php). These tools will allow us to extract the following data for our hitters:

    * Player rankings including their worst, best, and current rankings as well as their average draft position. 

    * Player bio including name, college, age, and they dominant hand when batting and throwing.

    * Players individual stats starting with the 2014 season and ending with the most recent season of 2020. The exception to this rule are players who haven't been in the league since 2014.

* Once our data is collected and cleaned, we will load our data into a relational database. In total we will have six different tables: "PITCHER_RANKINGS", "PITCHER_BIO", "PITCHER_STATS", "HITTER_RANKINGS", "HITTER_BIO", and "HITTER_STATS".

## Project Report

### **Extract:**

In **Jupyter Notebook** we used **Splinter**, **BeautifulSoup**, **Pandas**, and **ChromeDriverManager** to automate our data scraping process. We first created a table, with a player column to allow for merging/joining, for all starting pitcher and hitter rankings by reading in the html for the rankings table (pd.read_html). Then we executed a for loop that uses splinter to traverse through each link of our players to get their bio information as well as their season stats and created a list of dataframes. 

### **Transform:**

We transformed our data in our in two separate **Jupyter Notebooks**, one for hitters and another starting pitchers(SP), using both **Pandas** & **Python**. We created a for loops that will go through the list of dataframes for hitters and SP stats and append their respective stats into specific column headers. This will allow us to load our data into our relational database with ease. In the player stats dataframes we noted there were some players which were traded during a season creating nulls. We fixed this issue by using a forward fill function to replace the nulls with the preceding rows value.

### **Load:**

We loaded our datafames to **Postgres** using **SQLAlchemy** to append the data for starting pitchers' rankings, bio, and stats into their respective tables. We also used the same tools to append the data for hitters rankings, bio, and stats into their respective tables. All of our tables were created with **SQL** using schemas in **pgAdmin**. We added a truncated table function in order to prevent our data from being loaded more than once creating duplicate data in our tables. 

### **Important things to note:**

* The user must first set up variables under the Load section of the jupyter notebook in order to connect to their Postgres data base to match with the characters in bold using the following:
    * engine = create_engine(f'postgresql://{**username**}:{**password**}@localhost:5432/{**databasename**}')

* When first utilizing Splinter to scrape for our data we kept running into an error message and the program would stop. After some investigation we discovered we needed to accept cookies prior to  starting the process in order for it to run successfully. So we created a break in the program that prompts the user to accept cookies and input the amount of players to extract by rank. 

* We also found out quickly that **webdriver_manager.chrome** from ChromeDriverManager is very resource heavy so depending on user internet speed and computer specs it could cause lots of issues and cause the scraping to fail. A solution we found was to import **Time** and use the function time.sleep to allow our script enough time to scrape the data prior to proceeding to the next page. 

* Occasionally another issue we came across was there was a pop up add that would intermediately cause the program to stop. Since the data set we are dealing with is relatively small the user would manually close out the pop up... for larger datasets one could research a method that would allow our script to automatically close the pop up allowing script to continue running without errors.  

* If using the command line to run our python scripts use the following to avoid errors:

    * py -W ignore [PitcherScrape.py](PitcherScrape.py)

    * py -W ignore [HitterScrape.py](HitterScrape.py)
