# Version 2.0 of the JKE program
# This will be slightly more structured than the initial iteration
# 

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


def clean_existing_data():

    #First aim is to get the data from the csv to a dataframe and then move from there.
    xls = pd.ExcelFile('file') #careful with the file extensions when doing this!!!
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
        
        # need to sort out the non-runners, abandonments, fatalities and pulled up etc:
        if not str(row[4][0]).isnumeric():
            return 0
            
    
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
# seemingly obtaining the location and dates will be substantially more difficult due to the nature of the posts.    
def scrape_picks(blog_post_title):
    # Make a request
    page = requests.get(
        blog_post_title)
    soup = BeautifulSoup(page.content, 'html.parser')
    
    # We have a empty list to append the picks to. 
    # Similar to the above, we find all the paragraphs
    # We then check for the "@" symbol in the paragraph as this will hopefully be a horse together with its odds, or an email address.
    # .find method returns -1 if the string does not contain the desired character.
    # Then append the element to picklist. 
    # Currently we cannot find the location and require a method to do so. Also need the date and time. 
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
        pick[1] = pick[1].replace(" ","",1)
        
        if pick[1][-1] == " ":
            pick[1] = pick[1][:-1]
        
        pick_odds = []
        try:
            pick_odds = pick[1].split(" ")
            pick_odds[1] = True
        except:
            pick_odds[0] = pick[1]
            pick_odds.append(False)     # works provided no trailing spaces in second part of partition 
            
        print(pick_odds)
        pick.append(pick_odds[0])
        pick.append(pick_odds[1])
        pick.pop(1)
        print(pick)
        cleaned_picks.append(pick)
    print(cleaned_picks)    
    return(cleaned_picks)


# Function to parse the time_location list:
def parse_time_location(locations):
    # we parse at the first space to obtain the time and the location separately:
    cleaned_locations = []
    for location in locations:
        venue_list = location.split(" ",1)
        cleaned_locations.append(venue_list)
        
    print(cleaned_locations)
    return cleaned_locations
 

# Function to obtain the date of the blog post. Initially we will assume the horses run on the following day.        
def get_date(blog_post_title):
    page = requests.get(
        blog_post_title)
    soup = BeautifulSoup(page.content, 'html.parser')
    
    # IeNfNP
    dates = soup.find_all("li")
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
        race_date = datetime.today() - timedelta(days=int(days_ago[0]))  # gets the date of the race using one day after the blog was uploaded (optimal at present)
    except:
        dt = parse(days_ago) # this is for blogs posted with date as "Apr 6th" etc.
        print(dt)
        race_date = dt
    print(race_date.date())
    return race_date.date()
        
      
       
def get_location():
    return "Unknown"   

# This function will generate an object from each element in the list of cleaned picks: 
def generate_picks(cleaned_picks,cleaned_locations,date):
    
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
    
    def get_spreadsheet_odds(self,odds):
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
        odds_str = odds_str.split("/")
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
        
        df.loc[len(df)] = horse
        return
    
# function to save the df to a new spreadsheet:        
def df_to_sheet():
    with pd.ExcelWriter("file") as writer:
        df.to_excel(writer, sheet_name="Tipping sheet",index=False)


# RUN PROGRAM FROM HERE ::::
blog = ""

df = clean_existing_data()

#picks, locations = scrape_picks(blog)
#cleaned_picks = parse_picks(picks)
#cleaned_locations = parse_time_location(locations)
date = get_date(blog)
#generate_picks(cleaned_picks,cleaned_locations,date=get_date(blog))
df_to_sheet()
print(df)