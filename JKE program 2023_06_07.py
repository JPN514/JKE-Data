# Version 2.0 of the JKE program
# This will be slightly more structured than the initial iteration
# 

# Packages for the dataframe manipulation
import pandas as pd
import requests
import json
import time
import numpy as np
import datetime
from matplotlib import pyplot as plt
import seaborn as sns
import requests
from bs4 import BeautifulSoup
import datetime
from datetime import datetime, timedelta
from dateutil.parser import parse


# SQL PACKAGES: 
import mysql.connector
from mysql.connector import Error
from pandas.io import sql
#import MySQLdb
from sqlalchemy import create_engine
import pymysql                                             #unsure on this warning???
from python_SQL_functions import create_server_connection
from python_SQL_functions import create_database
from python_SQL_functions import create_db_connection
from python_SQL_functions import execute_query
from python_SQL_functions import read_query

# Function below cleans the existing Experimental tipping sheet V2, which was a bit of a mess.
def clean_existing_data():
    #First aim is to get the data from the csv to a dataframe and then move from there.
    xls = pd.ExcelFile('') #careful with the file extensions when doing this!!!
    df = pd.read_excel(xls,'Tipping sheet') #second argument tells us which sheet we are loading as a dataframe
    print(df.head())
    print(df.columns)
    #print(df)


    # We will drop some undesired columns and reindex the columns below:
    df = df.drop(["CALC ODDS", "Ordinal Places", "FORMULAE RETURNS (TOTAL)", "Return Excluding Stake"],axis=1)
    df = df.reindex(columns=["Date","Horse","Odds","E/W","Place","Points Ranking","Venue","Type","Time off","Runners","Paying Places","Place Odds","Stake","Return Including Stake"])
    print(df.columns)
    #print(df.dtypes)
    print(df.tail())


    # We sort out the NaN values and other gaps here:
    df["Time off"] = df["Time off"].fillna("12:00am")
    df["Venue"] = df["Venue"].fillna("Unknown")
    df["Type"] = df["Type"].fillna("Standard")
    df["Points Ranking"] = df["Points Ranking"].fillna(0)
    df["Stake"] = df["Stake"].fillna(1)
    df["Date"] = pd.to_datetime(df["Date"]).dt.date
    print(df.dtypes)

    # Need to sort out the odds column:

    #df["Odds"] = df["Odds"].fillna(0,inplace=True)
    df["Odds"] = df["Odds"].astype(str)             # changes to string
    mylambda = lambda x : 0 if x == "" else x       # fixes blanks
    df["Odds"] = df["Odds"].apply(mylambda)             

    mylambda = lambda x : x[5:10] if len(x) >= 5 else x   # since odds were entered as a date, we extract the relevant part
    df["Odds"] = df["Odds"].apply(mylambda)

    mylambda = lambda x : "1-1" if x == "EVS" else x      # ensures all odds are in consistent format
    df["Odds"] = df["Odds"].apply(mylambda)

    mylambda = lambda x : 0 if x == "nan" else x          # removes NaNs values
    df["Odds"] = df["Odds"].apply(mylambda)

    mylambda = lambda x : "0" if x == "" else x           # fixes blanks as more have arisen
    df["Odds"] = df["Odds"].apply(mylambda)

    df["Odds"] = df["Odds"].astype(str)                   # to string
    #print(df["Odds"].values)

    # since the odds were entered as a date, we need to take the day and month part, reverse them and reattach them below: 
    mylambda = lambda x : x.split("-")[1] + "-" + x.split("-")[0][1] if (x[0] == "0" and len(x) > 1) else x   #
    df["Odds"] = df["Odds"].apply(mylambda)

    mylambda = lambda x : x[1:] if x[0] == "0" else x      # removes the leading 0s
    df["Odds"] = df["Odds"].apply(mylambda)

    mylambda = lambda x : "0" if x == "" else x            # removes blanks
    df["Odds"] = df["Odds"].apply(mylambda)
    #print(df["Odds"].values)


    mylambda = lambda x : x if len(x) > 3 and x[3] == "0" else ""     # a fix for odds with "10" as second part e.g. 11/10 
    #print(df["Odds"].apply(mylambda).values)

    # ensures that odds containing "10" and "11" are written correctly:
    mylambda = lambda x : x.split("-")[1] + "-" + x.split("-")[0] if (x[3:5] == "11") else x   
    df["Odds"] = df["Odds"].apply(mylambda)

    mylambda = lambda x : "11-8" if x == "11-08" else x    # fix for 11/8
    df["Odds"] = df["Odds"].apply(mylambda)

    #print(df["Odds"].values)

    # Below we will fix the runners column (will alter races with unknown amount of runners to "unspecified".)
    mylambda = lambda x : "Unspecified" if x == "Winner" else x
    df["Runners"] = df["Runners"].apply(mylambda) 


    # Below will address the gaps in the returns column in the sheet.
    # This will involve using existing df columns to deduce the winnings for each horse.
    # The formula will be the analogous to the method in the horse class below.
    # function below will be applied to each row to generate the returns column:
    def generate_return(row):
    # here we need to ensure that all values from the sheet are numeric and in the appropriate format

        if row["Odds"] == 0 or row["Odds"] == "0":
            return 0
        odds = float(row["Odds"].split("-")[0]) / float(row["Odds"].split("-")[1]) # temporary conversion to calculation odds
        
        # need to sort out the non-runners, abandonments, fatalities and pulled up etc, the indices tell us we use the place column here:
        if not str(row[4][0]).isnumeric():
            return 0
            
        # Below if statement returns the winnings for the horse:
        if row[3] == "Yes":
            if row[4] == "1st":
                return (row[12] * 0.5 * odds) + (row[12] * 0.5 * row[11] * odds) + row[12]
            elif pd.to_numeric(row[4][0]) <= row[10]:
                return (row[12] * 0.5 * row[11] * odds) + (row[12] * 0.5)
            else: return 0
        elif row[4] == "1st":
            return (row[12] * odds) + row[12]
        else:
            return 0    

    # Apply the above function:
    df["Return Including Stake"] = df.apply(generate_return,axis=1)
    df.rename(columns={"Return Including Stake":"Returns"},inplace=True)                # rename column as returns is a fairer reflection

    # create a profit column:
    df["Profit"] = df["Returns"] - df["Stake"]
    
    return df




#SCRAPING BELOW:::

# change the blog post each time in the request section
# function below will scrape at least the name of the horse and the odds (including e/w) from a given blog post.   
def scrape_picks(blog_post_title):
    # Make a request (access the blog post)
    page = requests.get(
        blog_post_title)
    soup = BeautifulSoup(page.content, 'html.parser')
    
    # We have a empty list to append the picks to. 
    # Similar to the previous iteration of the program, we find all the paragraphs (p tags)
    # We then check for the "@" symbol in the paragraph as this will hopefully be a horse together with its odds, or an email address.
    # .find method returns -1 if the string does not contain the desired character, so we append the text to the list if .find is not -1
    
    # We use the index directly preceding that of the text with the "@" symbol to obtain the location and time of the race. This info is found on the line(paragraph) 
    # above the horse and odds, which is the standard style of the blog posts.
    # We then append the time and location to a list.
    picklist = []
    time_location = []
    for element in soup.find_all('p'):
        #print(element.text.find("@"))
        if element.text.find("@") != -1:
            index = soup.find_all('p').index(element) - 1
            picklist.append(element.text)
            time_location.append(soup.find_all('p')[index].text)
    
    # drop the last element of the picklist because it will be an email
    picklist.pop()
    # drop the last date_location element as it is paragraph preceding the email address
    time_location.pop()
    
    print(picklist)
    print(time_location)
    
    return picklist, time_location


# function below will separate the pick info so it is easier to add to the spreadsheet   
def parse_picks(picklist): 
    cleaned_picks = []
    # loop returns a list clean_picks containing lists of all parsed info (name, odds, e/w) on each pick.
    for pick in picklist:
        pick = pick.split("@")
        print(pick)
        pick[0] = pick[0][:len(pick[0])-1]
        pick[1] = pick[1].replace(" ","",1) # split on the "@", to get horse and odds seperate. Then remove the trailing and leading spaces.
        
        if pick[1][-1] == " ":
            pick[1] = pick[1][:-1] # remove trailing space if needed from the odds part
        
        pick_odds = []
        try:
            pick_odds = pick[1].split(" ") # deduce if the bet is each way, by trying to split on a space. Format is now "25-1 (E/W)".
            pick_odds[1] = True
        except:
            pick_odds[0] = pick[1]      # for non each way bets
            pick_odds.append(False)     # works provided no trailing spaces in second part of partition 
            
        print(pick_odds)
        pick.append(pick_odds[0])
        pick.append(pick_odds[1])
        pick.pop(1)                  # removes the tethered odds and eachway element
        print(pick)                  # pick now [horse,odds,True/False]
        cleaned_picks.append(pick)
    print(cleaned_picks)    
    return(cleaned_picks)


# Function to parse the time_location list:
def parse_time_location(locations):
    # we parse at the first space to obtain the time and the location separately:
    cleaned_locations = []
    for location in locations:
        venue_list = location.split(" ",1)
        try:
            if venue_list[1] == "":
                venue_list[1] = None
        except:
            venue_list.append(None)    
        
        if venue_list[1][0].isnumeric():    # this accounts for if the location and time are permuted in the blog post 
            time = venue_list[1]
            venue_list[1] = venue_list[0]
            venue_list[0] = time
            
        cleaned_locations.append(venue_list)  # locations now stored as [6.25pm, Pontefract] as an example.
    print(cleaned_locations)
    return cleaned_locations
 

# Function to obtain the date of the blog post. Initially we will assume the horses run on the following day.        
def get_date(blog_post_title):
    page = requests.get(
        blog_post_title)
    soup = BeautifulSoup(page.content, 'html.parser')
    
    # IeNfNP
    dates = soup.find_all("li") # searches the html for tags with the date of the blog.
    data = []
    #loop to iterate through each title in "titles" above
    for date in dates:
        item = {}
        item["Dates"] = date.find_all("span", class_="post-metadata__date time-ago") #F4tRtJ
        data.append(item)
    #print(data)
    
    # finds how many days ago the post was made:
    days_ago = soup.find("span", class_="post-metadata__date time-ago").text
    print(days_ago)
    try:
        race_date = datetime.today() - timedelta(days=int(days_ago[0])-1)  # gets the date of the race using one day after the blog was uploaded (optimal at present)
    except:
        dt = parse(days_ago) # this is for blogs posted with date as "Apr 6th" etc.
        race_date = dt + timedelta(days=1)
    print(race_date.date())
    return race_date.date()
        
      
       
def get_location():
    return "Unknown"   

# This function will generate an object from each element in the list of cleaned picks: 
def generate_picks(cleaned_picks,cleaned_locations,date):
    # passes the tuple of pick and location into the pick class to generate the data to append to the spreadsheet. 
    for item in zip(cleaned_picks,cleaned_locations):    
        print(item)   
        horse = pick(item,date)
        horse.append_horse()
        
        
#CLASS FOR HORSE BELOW ::: 

# class below will deal with each horse individually 
# this will hopefully ensure the process is smooth and clean
class pick():
    def __init__(self, cleaned_info, date):
        self.name = cleaned_info[0][0]
        self.spreadsheet_odds = self.get_spreadsheet_odds(cleaned_info[0][1])
        self.odds = self.get_calc_odds(cleaned_info[0][1])
        self.eachway = cleaned_info[0][2]
        self.date = date
        self.location = cleaned_info[1][1]
        #self.type = "Adverse Weather"
        self.time = cleaned_info[1][0]
        self.points_ranking = 0
        self.get_all_weather()
        self.get_info()
        self.print_info()
        return
    
    def get_spreadsheet_odds(self,odds): # to ensure consistency of odds format from df to spreadsheet 
        odds = odds.replace("/","-")
        #odds = "'" + odds
        return odds
    
# function to deduce whether the track/venue is all weather:
    def get_all_weather(self):
        # need a try/except and remember that place names can consist of multiple words!!!
        try:
            self.type = self.location.split("(")[1][0:2]
            self.location = self.location.split("(")[0][:-1]
            self.type = "All Weather"
        except:
            self.location = self.location
            self.type = "Standard"
        
        return    
        
    # get_info obtains other info pertinent to the horse in question:
    def get_info(self):
        
        if self.location == None:
            self.location = input("What track did {} race at? ".format(self.name))
        self.place = int(input("Where did {} place in the {} at {}? (Enter as an integer!): ".format(self.name,self.time,self.location)))
        self.runners = int(input("How many runners where there in the {} at {}? (Enter as an integer!): ".format(self.time,self.location)))
        self.paying_places = int(input("How many places are paying? (Enter as an integer!) "))
        self.place_odds = float(input("Enter the place odds e.g 0.2, 0.25: "))
        self.stake = 1
        self.returns_stake_inc = self.get_returns() 
       
    # function to print all the info to cross reference     
    def print_info(self):
        print(self.name,self.spreadsheet_odds,self.odds,self.eachway,self.date,self.location,self.place,self.runners,self.paying_places,self.place_odds,self.stake,self.returns_stake_inc)
        
    
    # function to return the calculation odds: 
    def get_calc_odds(self,odds_str):
        if odds_str == "evs" or odds_str == "evens":
            odds_str = "1/1"
        try:
            odds_str = odds_str.split("/")
        except:
            odds_str = odds_str.split("-")   
        return float(float(odds_str[0])/float(odds_str[1]))
    
    # function to determine the winnings or lack thereof for each pick: 
    def get_returns(self):
        # winnings are contingent on stake, odds, eachway and placing 
        if self.eachway == True:
            if self.place == 1:
                winnings = (self.stake * 0.5 * self.odds) \
                            + (self.stake * 0.5 * self.place_odds * self.odds) + self.stake
            elif self.place <= self.paying_places:
                winnings = (self.stake * 0.5 * self.place_odds * self.odds) \
                            + (self.stake * 0.5)
            else: 
                winnings = 0
        elif self.place == 1:            
            winnings = self.stake * self.odds + self.stake
        else:
            winnings = 0                 
        
        return winnings
    
    
    # function to prepare the data to be entered into the df
    # this includes changing the place from an integer to an ordinal and the odds to another format:
    def prep_horse_data(self):
        if self.eachway == True:
            self.eachway = "e/w"
        else: self.eachway = "No"
        
        if self.place == 1:
            self.place = "1st"
        elif self.place == 2:
            self.place = "2nd"    
        elif self.place == 3:
            self.place = "3rd"
        else:
            self.place = str(self.place) + "th"
                
        
            
    # function to add the new horse to the dataframe:
    def append_horse(self):
        self.prep_horse_data()
        
        horse = []
        horse.append(self.date)
        horse.append(self.name)
        horse.append(self.spreadsheet_odds)
        horse.append(self.eachway)
        horse.append(self.place)
        horse.append(self.points_ranking)
        horse.append(self.location)
        horse.append(self.type)
        horse.append(self.time)
        horse.append(self.runners)
        horse.append(self.paying_places)
        horse.append(self.place_odds)
        horse.append(self.stake)
        horse.append(self.returns_stake_inc)
        horse.append(self.returns_stake_inc-self.stake)
        
        df.loc[len(df)] = horse   # appends the horse to the end of the df. 
        return
    
# function to save the df to a new spreadsheet:        
def df_to_sheet():
    with pd.ExcelWriter("") as writer:
        df.to_excel(writer, sheet_name="Tipping sheet",index=False)

# This will load the cleaned data: 
def load_spreadsheet():
    xls = pd.ExcelFile('') #careful with the file extensions when doing this!!!
    df = pd.read_excel(xls,'Tipping sheet') #second argument tells us which sheet we are loading as a dataframe
    
    df = df.sort_values(by="Date")
    print(df.head())
    return df

# Function below will upload the dataframe to mySQL server: 
def df_to_mySQL(df):
    #This establishes connection to the mySQL server, can be commented out once we have database set up
    connection = create_server_connection("localhost", "root", "")
    
    #Creates the database called "" using the SQL query below       
    #create_database_query = "CREATE DATABASE ;"        #we can comment out this once the database has been created successfully!
    #testDB = create_database(connection,create_database_query)  
    
    #this connects to the database specified as the 4th parameter in the function call, in this case our Activities database
    db_connection = create_db_connection("localhost","root","","")
    
    create_table_query = """CREATE TABLE tipping_spreadsheet ();"""
    #create_table = execute_query(db_connection,create_table_query) #query to create the table
    #df.to_sql('tipping_spreadsheet', db_connection, if_exists='replace', index = False)
    #df.to_sql(con= db_connection, name='tipping_spreadsheet', if_exists='replace', flavor='mysql')
    
    db_connection = pymysql.connect(host='localhost',
                             user='root',
                             password='',
                             db='')
    # create sqlalchemy engine
    engine = create_engine("mysql+pymysql://{user}:{pw}@localhost/{db}"
                       .format(user="root",
                               pw="",
                               db=""))
    # create cursor
    cursor = db_connection.cursor()
    
    # Insert whole DataFrame into MySQL
    df.to_sql('tipping_spreadsheet', con = engine, if_exists = 'replace', chunksize = 1000)
    
    select_all_query = """SELECT * FROM tipping_spreadsheet ORDER BY Date ASC;"""
    select_all_data = read_query(db_connection,select_all_query)
    print(select_all_data)
    
    select_count_query = """SELECT COUNT(*) FROM tipping_spreadsheet;"""
    select_count_data = read_query(db_connection,select_count_query)
    print(select_count_data)


# RUN PROGRAM FROM HERE ::::
# Note check format of odds in blog post for "/" or "-"
blog = ""
#df = clean_existing_data()


df = load_spreadsheet()
picks, locations = scrape_picks(blog)
#cleaned_picks = parse_picks(picks)
cleaned_locations = parse_time_location(locations)
#date = get_date(blog)
#generate_picks(cleaned_picks,cleaned_locations,date=get_date(blog))


#print(df)
df_to_sheet() 
#df_to_mySQL(df)
