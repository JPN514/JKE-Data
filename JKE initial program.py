#Initial program for JKE work and experiments.

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


#First aim is to get the data from the csv to a dataframe and then move from there.
xls = pd.ExcelFile('file') #careful with the file extensions when doing this!!!
df = pd.read_excel(xls,'Tipping sheet') #second argument tells us which sheet we are loading as a dataframe
print(df.head())
print(df.columns)
#print(df)

#Function to hopefully webscrape the picks from the website's blogs for entry into the spreadsheet:
def first_scrape():

    # Make a request
    page = requests.get(
        "")
    soup = BeautifulSoup(page.content, 'html.parser')

    # Extract first <h1>(...)</h1> text
    first_h1 = soup.select('span')[0].text
    print(first_h1)
    
    # Create all_h1_tags as empty list
    all_h1_tags = []

    # Set all_h1_tags to all h1 tags of the soup
    for element in soup.select('h1'):
        all_h1_tags.append(element.text)
        #print(element.text)
    
    # Create p_text and attempt to find index of the picks (horse name and odds). 
    # In this case one pick is an the 12th index.
    seventh_p_text = soup.select('p')[12].text
    #print(all_h1_tags, seventh_p_text)
    
    # This loop prints each paragraph of text, maybe we can parse from this?
    # Prints as the title of the blog post and the paragraph of text: 
    # ['Crowning a King and Queen'] However, these races over the Rowley Mile will crown the King and Queen of the turf on this momentous weekend.
    for i in range(0,len(soup.find_all("p"))):
        print(all_h1_tags, soup.select('p')[i].text)
    
    # We have a empty list to append the picks to. 
    # Similar to the above, we find all the paragraphs
    # We then check for the "@" symbol in the paragraph as this will hopefully be a horse together with its odds, or an email address.
    # .find method returns -1 if the string does not contain the desired character.
    # Then append the element to picklist. 
    # Currently we cannot find the location and require a method to do so. Also need the date and time. 
    picklist = []
    for element in soup.find_all('p'):
        #print(element.text.find("@"))
        if element.text.find("@") != -1:
            picklist.append(element.text)
    print(picklist)
        
    #extracts all the text from the webpage, this is probably what we need. 
    text = soup.get_text() 
    #print(text)
    return 
    
    
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
    for element in soup.find_all('p'):
        #print(element.text.find("@"))
        if element.text.find("@") != -1:
            picklist.append(element.text)
    
    # drop the last element of the picklist because it will be an email
    picklist.pop()
    print(picklist)
    
    return picklist

# function below will separate the pick info so it is easier to add to the spreadsheet   
def parse_picks(picklist): 
    cleaned_picks = []
    # loop returns a list clean_picks containing lists of all parsed info (name, odds, e/w) on each pick.
    for pick in picklist:
        pick = pick.split("@")
        print(pick)
        pick[0] = pick[0][:len(pick[0])-1]
        pick[1] = pick[1].replace(" ","",1)
        pick_odds = []
        pick_odds = pick[1].split(" ")
        print(pick_odds)
        pick.append(pick_odds[0])
        pick.append(pick_odds[1])
        pick.pop(1)
        print(pick)
        cleaned_picks.append(pick)
    print(cleaned_picks)    
    return(cleaned_picks)
        
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
    return days_ago
        
def get_location():
    return "Unknown"    


# this function will prepare the picks for entry into the spreadsheet:
def prep_picks(picks,date_ran):
    # this loop gets the place and paying places etc:
    for pick in picks:
        formatted_pick = []
        formatted_pick.append(date_ran) # date
        formatted_pick.append(pick[0]) # horse
        formatted_pick.append(pick[1]) # odds
        calc_odds = pick[1].split("/")
        formatted_pick.append(float(float(calc_odds[0])/float(calc_odds[1]))) #CALC odds
        formatted_pick.append("7:00pm") # time
        ordinal_place = input("Enter where {} placed as an ordinal: ".format(pick[0])) # ordinal place
        formatted_pick.append(ordinal_place)
        formatted_pick.append(int(input("Enter number of runners in {}'s race: ".format(pick[0])))) # runners
        formatted_pick.append(pick[2]) # e/w
        paying_places = int(input("Enter number of paying places in {}'s race: ".format(pick[0]))) 
        formatted_pick.append(paying_places) # paying places
        formatted_pick.append(get_ordinal(paying_places)) # ordinal places
        formatted_pick.append(input("Enter the odds for the paying places: ".format(pick[0]))) # place odds
        formatted_pick.append(1) # points rank
        formatted_pick.append(get_location()) # venue
        formatted_pick.append("Unknown") # A/W
        formatted_pick.append(1) # stake
        formatted_pick.append(0) # return ex stake
        formatted_pick.append(0) # return inc stake
        formatted_pick.append(0) # formulae returns
        
        print(formatted_pick)
        # now write the pick to the dataframe:
        append_to_df(formatted_pick)
        
    return

def formulae_returns():
    pass
        
        
# function to turn the place into an ordinal place for the sheet:         
def get_ordinal(place):
    if place == 1:
        return "1st"
    elif place == 2:
        return "2nd"
    elif place == 3:
        return "3rd"
    else:
        return (str(place) + "th")
        
        
          
# function to create a dataframe for each pick to aid entry into the spreadsheet:
def append_to_df(formatted_pick):
    pick_df = pd.DataFrame([formatted_pick], columns=df.columns)   
    print(pick_df.head())
    write_pick_to_sheet(pick_df)
    return

# function to write the dataframe of an individual pick to the spreadsheet: 
def write_pick_to_sheet(pick_df):
    with pd.ExcelWriter("file",engine="openpyxl",mode="a",if_sheet_exists="overlay") as writer:
        pick_df.to_excel(writer, sheet_name="Tipping sheet",header=None, startrow=writer.sheets["Tipping sheet"].max_row,index=False)
          
#first_scrape()   

title = ""
#picks = scrape_picks(title)
#cleaned_picks = parse_picks(picks)
#date_ran = get_date(title)
#prep_picks(cleaned_picks,date_ran)

