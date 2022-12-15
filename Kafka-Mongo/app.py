import pymongo
import numpy as np
import datetime

# # start mongo client
client = pymongo.MongoClient('localhost', 27017)

# # connect to itc6107 database
db = client.itc6107


# # the strip in all the lines is used to correct users miss-type of a empty character. I split the range of dates
# # on the - to get a list of two items(the two dates ,starting date in position 0 of the list and ending in the
# # position 1. Then i make split again to make a list of 3 elements. In 0 position the day, in 1 the month and in
# # 2 the year, and i make one for the starting date and one for the ending. I make them integers so i can process them
# # but also get rid of the 0 on the start of a number (03). Then i validate the dates to see if they exist and it is
# # not wrong dates but also make them in isoformat(same format as the entry of dates in mongodb from the consumer files
# # In order to check the dates i have to provide them in the format '%Y-%m-%d'(same as the entry), so i turn them
# # into strings with dashes in between. Finally i check if the dates are in the range required by the exercise.
def date_validate(date_range):
    two_dates = date_range.strip().split('-')
    starting = two_dates[0].strip().split('/')
    ending = two_dates[1].strip().split('/')

    starting_day = int(starting[0].strip())
    starting_month = int(starting[1].strip())
    starting_year = int(starting[2].strip())
    ending_day = int(ending[0].strip())
    ending_month = int(ending[1].strip())
    ending_year = int(ending[2].strip())

    start_date = datetime.datetime.strptime(str(starting_year) + '-' + str(starting_month) + '-' + str(starting_day),
                                            '%Y-%m-%d').isoformat()
    end_date = datetime.datetime.strptime(str(ending_year) + '-' + str(ending_month) + '-' + str(ending_day),
                                          '%Y-%m-%d').isoformat()
    # # Required range: 1/1/1970 - 9/4/2021
    # # In every 'if' i check to see that the starting date is before the ending date and that the starting year is
    # # greater than the required. This combined with the ,if starting year is greater than 1970 and smaller than 2021
    # # we accept everything and return the 2 dates. Only left to investigate what happens if the ending year is 2021.
    if start_date < end_date and starting_year >= 1970 and ending_year < 2021:
        return start_date, end_date

    # # if the ending year is 2021 and the month is smaller than april we again can accept any entry.I return the dates.
    # # no we have to see what happens when 2021 april.
    elif start_date < end_date and starting_year >= 1970 and ending_year == 2021 and ending_month < 4:
        return start_date, end_date

    # # if the year is 2021 and the ending month is april(4) then the day must be lower or equal than 10.
    # # the ending day is lower than 10 and not 9 so that you can also query the 9th of april
    # # i don t provide same restrictions for the starting days and months because i require always start_date<end_date
    elif start_date < end_date and starting_year >= 1970 and ending_year == 2021 and ending_month == 4 and ending_day <= 10:
        return start_date, end_date
    else:
        return False


# # here i provide the input to enter the date range until you enter a valid one.
# # Valid date ranges has the format: dd/mm/yyyy-dd/mm/yyyy (first the starting, then the ending date for the search)
# # if you want to query only for 1 day, you have to provide for example 9/4/2021-10/4/2021 to see results only for
# # the ninth of april 2021. Try to check if the dates are valid, if not the function will blow and you will return
# # to the input function. if you enter a valid date, the return of the date_validate function(a tuple) is returned
# # and saved in the variable tuple_date (first element the starting and second the ending date) and you break out
# # the while true loop.
while True:
    query = input('Enter starting and ending date in the format dd/mm/yyyy-dd/mm/yyyy : ')

    try:
        tuple_date = date_validate(query)
        if tuple_date:
            break
        else:
            print('Wrong format or date, please try again..')
            continue

    except:
        print('Wrong format or date, please try again..')

# print(tuple_date)
print('Starting date for the query is: ', tuple_date[0])
print('Ending date for the query is: ', tuple_date[1])

# # In both queries i use aggregate. On Question 1 i match all the documents that their 'ts' ( timestamps )
# # date of their entry is greater or equal than the start date you entered (tuple_date[0]) and lower or equal
# # than the end date you provided. This on default projects all the fields which is the required.
Question1 = db.StockExchange.aggregate([
    {'$match': {'ts': {'$gte': tuple_date[0], '$lte': tuple_date[1]}}}
])

# # i iterate through the results and print them
print('All entries within the given range: ')
for i in Question1:
    print(i)

# # Here same as before i match ( i filter and get only the documents that the match statement is full filled)
# # all the documents that their spread is greater than 3 and their ts is between the starting and ending date as before
# # I get the whole documents back so i project ( so to the output) only the spread, ts and tick features
Question2 = db.StockExchangeC.aggregate([
    {
        "$match": {
            "spread": {'$gt': 3},
            "ts": {
                "$gte": tuple_date[0],
                "$lte": tuple_date[1]
            }
        }
    }, {"$project": {
        "spread": 1, "tick": 1, "ts": 1
    }}
])

# # i iterate through the results and print them
print('All entries within the given range: ')
for i in Question2:
    print(i)
