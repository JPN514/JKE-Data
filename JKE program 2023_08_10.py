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
from python_SQL_functions import df_to_mySQL
from python_SQL_functions import get_connection
from python_SQL_functions import get_engine
db_connection = get_connection()
engine = get_engine()

import webbrowser
# getting path
chrome_path = r"C:\Program Files\Google\Chrome\Application\chrome.exe"
# First registers the new browser
webbrowser.register('chrome', None, 
                    webbrowser.BackgroundBrowser(chrome_path))



# Function below cleans the existing Experimental tipping sheet V2, which was a bit of a mess.
def clean_existing_data():
    #First aim is to get the data from the csv to a dataframe and then move from there.
    xls = pd.ExcelFile('TIPS_Experimental_V2.xlsx') #careful with the file extensions when doing this!!!
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
        try:
            venue_list = location.split(" ",1)
        except: pass     
        
        try:
            if venue_list[1] == "":
                venue_list[1] = None
        except:
            venue_list.append(None)
                
        try:
            if venue_list[1][0].isnumeric():    # this accounts for if the location and time are permuted in the blog post 
                time = venue_list[1]
                venue_list[1] = venue_list[0]
                venue_list[0] = time
        except: pass
                
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
        self.chrome_search() 
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
    
    
    # function to open a chrome tab to search for missing data:
    def chrome_search(self):
        
        url = "https://www.google.com/search?q=at+the+races+" + str(self.location) + "+" + str(self.date.day) + "+" + str(self.get_month()) + "+" + str(self.date.year) 
        webbrowser.get('chrome').open(url)
        return 
    
    def get_month(self):
        if self.date.month == 1:
            return "January"
        elif self.date.month == 2:
            return "February"
        elif self.date.month == 3:
            return "March"
        elif self.date.month == 4:
            return "April"
        elif self.date.month == 5:
            return "May"
        elif self.date.month == 6:
            return "June"
        elif self.date.month == 7:
            return "July"
        elif self.date.month == 8:
            return "August"
        elif self.date.month == 9:
            return "September"
        elif self.date.month == 10:
            return "October"
        elif self.date.month == 11:
            return "November"
        elif self.date.month == 12:
            return "December"
        


# ::::: SPREADSHEET AND SQL AND DATA FUNCTIONS BELOW :::::
    
# function to save the df to a new spreadsheet:        
def df_to_sheet():
    with pd.ExcelWriter("TIPS_Experimental_V3.xlsx",engine='openpyxl',mode='a',if_sheet_exists='replace') as writer:
        df.to_excel(writer, sheet_name="Tipping sheet",index=False)

# This will load the cleaned data: 
def load_spreadsheet():
    xls = pd.ExcelFile('TIPS_Experimental_V3.xlsx') #careful with the file extensions when doing this!!!
    df = pd.read_excel(xls,'Tipping sheet') #second argument tells us which sheet we are loading as a dataframe
    
    df = df.sort_values(by="Date")
    print(df.head())
    return df


# function to corroborate total stakes and returns:
def check_totals(df):
    print(np.sum(df.Stake))
    print(np.sum(df.Returns))
    print(np.sum(df.Profit))
    
    # plot the bar chart below:
    values = [np.sum(df.Stake),np.sum(df.Returns),np.sum(df.Profit)]
    labels = ["Stakes","Returns","Profits"]
    plt.bar(range(3),values,color = "purple")
    plt.title("JKE MBC")
    plt.ylabel("£ Sterling")
    ax = plt.subplot()
    ax.set_xticks(range(3))
    ax.set_xticklabels(labels)
    plt.show()
    return

# function to drop my fictional test horses from the df (ONLY USE ONCE):
def delete_swindon_horses(df):
    mylambda = lambda row : False if row == "Swindon" else True # finds where the venue is "Swindon" and assigns false since the df only indexes the true values.
    df = df[df.Venue.apply(mylambda)].reset_index(drop=True)
    print(len(df))
    print(df)
    return df
    
# function to create bar/pie for outright to eachway bets: 
def outright_vs_eachway():
    # Clean the eachway column entries:
    mylambda = lambda x : "Yes" if x == "yes" else x
    df["E/W"] = df["E/W"].apply(mylambda)
    print(df["E/W"].unique())
    
    # first count from the df:
    outright = df[df["E/W"] == "No"].Horse.count()
    eachway = df[df["E/W"] == "Yes"].Horse.count() + df[df["E/W"] == "e/w"].Horse.count()
    print(outright,eachway) 
    
    # Plt plot for the barchart:
    plt.bar(range(2),[outright,eachway],color="green")
    plt.title("Outright Vs Each-way Bets")
    plt.xlabel("Type of Bet")
    plt.ylabel("Number of Bets")
    ax = plt.subplot()
    ax.set_xticks(range(2))
    ax.set_xticklabels(["Outright","Each-way"])
    plt.show() 
    plt.clf()
    labels = ["Outright","Each-way"]
    
    # plt pie chart:
    plt.pie([outright,eachway],autopct="%0.1f%%")
    plt.axis("equal")
    plt.title("Outright Vs Each-way Bets")
    plt.legend(labels,loc=3)
    plt.show() 
      
    return


# function to acquire stacked bars for winners and places and losers by month:
def earners_by_month(df):
    df["Month"] = df.Date.dt.month
    df["Year"] = df.Date.dt.year
    months = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug"]
    df = df[df.Year == 2023] 
    df = df[df.Returns > 0] # remove all losing horses
    
    winners = df[df["Place"] == "1st"].groupby("Month").Place.count()
    places = df[df["Place"] != "1st"].groupby("Month").Place.count()
    print(winners,places)
    legend_labels = ["Places","Winners"]
    
    plt.bar(range(0,8), places)
    plt.bar(range(0,8),winners,bottom=places)
    plt.locator_params(axis="y", integer=True, tight=True) # ensures values are integers and not floats !!!
    plt.title("Winners And Places In 2023")
    plt.xlabel("Month")
    plt.ylabel("Horse Count")
    ax = plt.subplot()
    ax.set_xticks(range(0,8))
    ax.set_xticklabels(months)
    plt.legend(legend_labels)
    plt.show()
    
    
     


# function to rectify issues with non-runners and abandoned races
# Previously this was affecting the returns and profits:
def clean_places(df):
    print(df.Place.unique())
    df["Stake"] = df.apply(change_stake,axis=1) # will go thorugh each row and alter stake if required.
    df["Returns"] = df.apply(change_returns,axis=1)
    df["Profit"] = df.apply(change_profit,axis=1)
    print(df)
    
    non_runners = df[(df.Place == "NON Runner") | (df.Place == "ABANDONED") | (df.Place == "CALLED OFF")]
    print(non_runners,len(non_runners))
    return
# helper functions for the above 
def change_stake(row):
    if row[4] == "NON Runner" or row[4] == "ABANDONED" or row[4] == "CALLED OFF":
        return 0
    else: return row[12]
def change_returns(row):
    if row[4] == "NON Runner" or row[4] == "ABANDONED" or row[4] == "CALLED OFF":
        return 0
    else: return row[13] 
def change_profit(row):
    if row[4] == "NON Runner" or row[4] == "ABANDONED" or row[4] == "CALLED OFF":
        return 0
    else: return row[14]
    

# function to derive returns by year from the tipping sheet:
def yearly_returns(df):
    
    df_year = df[["Date","Stake","Returns","Profit"]]       # list of columns to create mini df from main df
    df_year.Date = df_year.Date.dt.year
    print(df_year.Date.unique())
    df_year = df_year.groupby("Date").sum("Returns")
    print(df_year.head())
    
    return

# function which uses SQL to obtain running totals for returns/profits for 2023, careful with the commented out code:
def running_totals(df):
    df_2023 = df[["Date","Stake","Returns","Profit"]] # first select the 2023 information
    df_2023["Year"] = df_2023.Date.dt.year
    df_2023["Month"] = df_2023.Date.dt.month 
    
    # use SQL to derive totals then append to sheet:
    totals = running_totals_to_mysql(df_2023)
    running_totals_to_sheet(totals)
    
    #print(df_2023)
    return
def running_totals_to_mysql(df_2023):
    # sql to extract the running totals for 2023:
    df_2023.to_sql('totals_2023', con = engine, if_exists = 'replace', chunksize = 1000)
    select_all_query = """SELECT * FROM totals_2023;"""
    select_all = read_query(db_connection,select_all_query)
    #print(select_all)
    
    query_monthly_totals = """WITH month_totals AS (
                SELECT Month, SUM(Stake) as Monthly_Stake, SUM(Returns) AS Monthly_Returns,
                SUM(Profit) AS Monthly_Profit
                FROM totals_2023
                WHERE Year = 2023
                GROUP BY Month)
                SELECT Month, Monthly_Stake, Monthly_Returns,
                SUM(Monthly_Stake) OVER (
                ORDER BY Month) AS "running_stakes", 
                SUM(Monthly_Returns) OVER (
                ORDER BY Month) AS "running_returns", 
                SUM(Monthly_Profit) OVER (
                ORDER BY Month) AS "running_profits"
                FROM month_totals
                GROUP BY Month;"""
    result = read_query(db_connection,query_monthly_totals)
    #print(result)
    result_list = [*result]
    #print(result_list)
    result_series = pd.DataFrame(result_list,columns=["Month","Monthly Stakes","Monthly Returns","Overall Stakes","Overall Returns","Overall Profit"])
    #result_series = result_series.drop(index=len(result_series)-1) # we do not want to include the incomplete month
    print(result_series)
    return result_series
def running_totals_to_sheet(totals):
    # function to append the running totals for 2023 to the file, could probably make this cleaner.
    with pd.ExcelWriter("TIPS_Experimental_V3.xlsx",engine='openpyxl',mode='a',if_sheet_exists='replace') as writer:
        totals.to_excel(writer, sheet_name="Running Totals 2023",index=False)


# function to return largest returns (winners and each-way):
def get_top_earners(n):
    top_n = df.sort_values("Returns",ascending=False).head(n).reset_index()
    print(top_n)
    earners = df[df["Profit"] > 0].sort_values("Profit",ascending=False).reset_index()
    dated_earners = earners[["Date","Horse","Odds","E/W","Place","Venue","Profit"]].sort_values("Date",ascending=False)
    earners = earners[["Horse","Odds","E/W","Place","Venue","Profit"]].sort_values("Profit",ascending=False)
    print(len(earners))
    #print(dated_earners)
    with pd.ExcelWriter("TIPS_Experimental_V3.xlsx",engine='openpyxl',mode='a',if_sheet_exists='replace') as writer:
        earners.to_excel(writer, sheet_name="Earners",index=False)
    return dated_earners




# function to rectify issues with each-way horses pre auotmation
# each-way horses placing 10th or below showing erroneous profits in sheet
# THIS CAUSED MANY MANY ISSUES !!!!
def fix_eachway(df):
    # firstly obtain the horses with the erroneous returns and profits:
    print(df["E/W"].unique())
    eachway = df[(df["E/W"] == "Yes") | (df["E/W"] == "e/w")]
    eachway = eachway[eachway.Profit > 0]
    eachway = eachway[(eachway.Place == "10th") | (eachway.Place == "11th") | (eachway.Place == "12th") | (eachway.Place == "13th") | (eachway.Place == "14th")] 
    #print(eachway)     
    problem_horses = eachway
    problem_horses.Returns = problem_horses.Returns.astype("float")
    problem_horses.Profit = problem_horses.Profit.astype("float")
    print(problem_horses, problem_horses.Returns.sum(), problem_horses.Profit.sum())
    
    # alter the profit and returns:
    mylambda = lambda x : 0 if x > 0 else x
    eachway.Returns = eachway.Returns.apply(mylambda)
    mylambda = lambda x : -1 if x > 0 else x
    eachway.Profit = eachway.Profit.apply(mylambda)
    print(eachway)
    #print(eachway.Profit.unique())
    
    # merge the dataframes together:
    eachway.Returns = eachway.Returns.astype("float") # alter datatypes to ensure neatness
    eachway.Profit = eachway.Profit.astype("float")
    print(df.dtypes, eachway.dtypes)
    outer_merge = df.merge(eachway,how="outer")        # use outer merge to get all data together from both frames 
    #final_merge = outer_merge.drop_duplicates(problem_horses) # drop the problem horses based on their indicies, which will be the same in the merged frame
    
    
    idx1 = set(outer_merge.set_index(["Date","Horse","Odds","E/W","Place","Points Ranking","Venue","Type","Time off","Runners","Paying Places","Place Odds","Stake","Returns","Profit"]).index)
    idx2 = set(problem_horses.set_index(["Date","Horse","Odds","E/W","Place","Points Ranking","Venue","Type","Time off","Runners","Paying Places","Place Odds","Stake","Returns","Profit"]).index)

    final_merge = pd.DataFrame(list(idx1 - idx2), columns=df.columns).sort_values(by="Date").reset_index()
    final_merge = final_merge.drop(columns="index")
    print(final_merge)
    return final_merge


# 2nd attempt at producing function to rectify issues with each-way horses pre auotmation
# each-way horses placing 10th or below showing erroneous profits in sheet
def fix_eachway2():
    
    # firstly obtain the horses with the erroneous returns and profits:
    print(df["E/W"].unique())
    eachway = df[(df["E/W"] == "Yes") | (df["E/W"] == "e/w")]
    eachway = eachway[eachway.Profit > 0]
    eachway = eachway[(eachway.Place == "10th") | (eachway.Place == "11th") | (eachway.Place == "12th") | (eachway.Place == "13th") | (eachway.Place == "14th")] 
    #print(eachway)     
    problem_horses = eachway
    problem_horses.Returns = problem_horses.Returns.astype("float")
    problem_horses.Profit = problem_horses.Profit.astype("float")
    print(problem_horses, problem_horses.Returns.sum(), problem_horses.Profit.sum())
    






# RUN PROGRAM FROM HERE ::::
# Note check format of odds in blog post for "/" or "-"
blog = ""
#df = clean_existing_data()


df = load_spreadsheet()
#picks, locations = scrape_picks(blog)
#cleaned_picks = parse_picks(picks)
#cleaned_locations = parse_time_location(locations)
#date = get_date(blog)
#generate_picks(cleaned_picks,cleaned_locations,date=get_date(blog))



# GRAPHS AND TOTALS:
#clean_places(df)
#df = delete_swindon_horses(df)
#df = fix_eachway(df)
#fix_eachway2()
print(len(df))
#check_totals(df)
#outright_vs_eachway()
#yearly_returns(df) # only run after altering date format within sheet !!! 
#running_totals(df)
#get_top_earners(105)
earners_by_month(df)


# SQL AND SAVING TO FILE: 
#print(df)
#df["Date"] = pd.to_datetime(df["Date"]).dt.date
#df_to_sheet() 
#df_to_mySQL(df) #ONLY RUN ONCE CONVINCED ALL IS CORRECT !!!


print(np.sum(df.Stake))
print(np.sum(df.Returns))
print(np.sum(df.Profit))

