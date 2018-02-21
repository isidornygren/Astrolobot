#!/usr/bin/env python
""" collector.py: Fetches and parses data from horoscope databases """

__author__      = "Isidor Nygren"
__copyright__   = "Copyright 2018, Isidor Nygren"
__license__     = "MIT"
__version__     = "1.0"

from lxml import html
from datetime import date
from datetime import timedelta
from datetime import datetime
from pathlib import Path
import sys, getopt
import requests
import re
import csv

class Collector:
    star_signs = {'aries': 1,'taurus': 2, 'hemini': 3,'cancer': 4,'leo': 5,'virgo': 6,'libra': 7,'scorpio': 8,'sagittarius': 9,'capricorn': 10,'aquarius': 11,'pisces': 12}
    database_name = "database"

    @staticmethod
    def run(start_date, end_date):
        """
        Parses horoscopes from start_date until end_date for all signs and outputs
        them into a database-YYYY-mm-dd.csv file

        Parameters
        ----------
        start_date : date
            starting date to fetch horoscopes from
        end_date : date
            the end date where to fetch horoscopes from
        """
        db_date = datetime.now().strftime("%Y-%m-%d")
        days = (end_date - start_date).days + 1
        Collector.__print("Executing collector")
        for day in range(0,days):
            date = start_date + timedelta(days=day)
            Collector.__print("Running for date: " + date.strftime("%Y-%m-%d"))
            for sign in range(1,13):
                try:
                    Collector.__print("Running for sign: " + str(sign))
                    horoscope = Collector.__get_horoscope(date, sign)
                    Collector.__append_to_csv(horoscope, Collector.database_name + "-" + db_date + ".csv")
                except FileNotFoundError as e:
                    print(e)
                    # This break assumes if no the first sign does not exist, no other sign exists
                    break

    @staticmethod
    def __print(string):
        time = datetime.now().strftime("%H:%M")
        print(time + ' [Collector]: ' + string)

    @staticmethod
    def __append_to_csv(horoscope, filename):
        """
        Adds a horoscope to a csv file, or creates a new csv file with the horoscope

        Parameters
        ----------
        horoscope : table
            horoscope object containing a str (horoscope), date (date) and integer (sign)
        filename : str
            name of the database to add to horoscope to
        """
        filexists = Path(filename).is_file()
        with open(filename, 'a') as csvfile:
            fieldnames = ['horoscope', 'sign', 'date']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            if not filexists:
                writer.writeheader()
            writer.writerow({'horoscope':horoscope['horoscope'], 'sign':horoscope['sign'] , 'date':horoscope['date'].strftime("%Y-%m-%d")})

    @staticmethod
    def __get_horoscope(date, sign):
        """
        Fetches and parses a horoscope from horoscope.com

        Parameters
        ----------
        date : date
            The date to parse the horoscope from
        sign : int or str
            the sign to get the horoscope from, either in a string form eg "cancer"
            or as an integer ranging from 1 = aries to 12 = pisces.
        """
        date_string = date.strftime("%Y%m%d")
        if isinstance(sign, str):
            sign_n = str(Collector.star_signs[sign.lower()])
        elif isinstance(sign, int) and sign >= 0 and sign <= 12:
            sign_n = str(sign)
        else:
            raise TypeError('Wrong sign type, expected str or int, got ' + str(type(sign)) + '.')

        url = "https://www.horoscope.com/us/horoscopes/general/horoscope-archive.aspx?sign=" + sign_n + "&laDate=" + date_string
        response = requests.get(url)
        tree = html.fromstring(response.content)

        # Extract elements from html
        horoscope_text = str(tree.xpath("//div[contains(@class, 'horoscope-content')]/p/text()"))
        horoscope_date = str(tree.xpath("//div[contains(@class, 'horoscope-content')]/p/b/text()"))

        # Remove artifacts from text
        horoscope_text = horoscope_text.replace('[\'\\n\', " - ', ''). replace('\\n"]', '').replace('\\n", "Who\'s in your future? ", "Who\'s in your future? "]', '')
        horoscope_date = horoscope_date.replace('[\'', '').replace('\']', '')

        # If the date did not exist on the website
        date_compare = date.strftime("%b %#d, %Y")
        if horoscope_date != date_compare:
            print('"' + horoscope_date + '"')
            print('"' + date_compare + '"')
            raise FileNotFoundError('No horoscope from {} found.'.format(date_compare))
        return {'date': date, 'horoscope': horoscope_text, 'sign': sign_n}

def main(argv):
    startdate   = "2018-01-01"
    enddate     = "2018-01-10"
    try:
        opts, args = getopt.getopt(argv, "hs:e:",["startdate=", "enddate="])
    except getopt.GetoptError:
        print('collector.py -s YYYY-MM-DD -e YYYY-MM-DD')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print('collector.py -s YYYY-MM-DD -e YYYY-MM-DD')
            sys.exit()
        elif opt == '-s':
            startdate = arg
        elif opt == '-e':
            enddate = arg
    Collector.run(datetime.strptime(startdate, '%Y-%m-%d').date(), datetime.strptime(enddate, '%Y-%m-%d').date())

if __name__ == "__main__":
    main(sys.argv[1:])
