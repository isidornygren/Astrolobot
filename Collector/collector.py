#!/usr/bin/env python
""" collector.py: Fetches and parses data from horoscope databases """

__author__ = "Isidor Nygren"
__copyright__ = "Copyright 2018, Isidor Nygren"
__license__ = "MIT"
__version__ = "1.1"

from lxml import html
from datetime import date
from datetime import timedelta
from datetime import datetime
from pathlib import Path
from multiprocessing.dummy import Pool as ThreadPool
import argparse
import requests
import re
import csv
import itertools
import os
import random
import moon


class Collector:
    star_signs = {
        "aries": 1,
        "taurus": 2,
        "hemini": 3,
        "cancer": 4,
        "leo": 5,
        "virgo": 6,
        "libra": 7,
        "scorpio": 8,
        "sagittarius": 9,
        "capricorn": 10,
        "aquarius": 11,
        "pisces": 12,
    }
    database_name = "horoscopes_db"
    # Formatting for day in date formatting for different os's
    day_single = "%#d" if os.name == "Windows" else "%-d"

    @staticmethod
    def run(start_date, end_date, test_perc):
        """
        Parses horoscopes from start_date until end_date for all signs and outputs
        them into a database_name-YYYY-mm-dd.csv file

        Parameters
        ----------
        start_date : date
            starting date to fetch horoscopes from
        end_date  : date
            the end date where to fetch horoscopes from
        test_perc : int
            the percentage of the data to turn into a separate test database (0-100)
        """
        starting_time = datetime.now()
        db_date = starting_time.strftime("%Y-%m-%d")
        days = (end_date - start_date).days + 1
        Collector.__print("Executing collector")

        tot_horoscopes = 0

        for day in range(0, days):
            date = start_date + timedelta(days=day)
            Collector.__print("Running for date: " + date.strftime("%Y-%m-%d"))
            pool = ThreadPool(12)
            results = pool.starmap(
                Collector.get_horoscope_thread,
                zip(itertools.repeat(date), [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]),
            )

            pool.close()
            pool.join()

            tot_horoscopes += results.__len__()
            print("Results length: " + str(results.__len__()))
            for result in results:
                if result is not None:
                    rand_int = random.randint(1, 100)
                    if rand_int > int(test_perc):
                        Collector.__append_to_csv(
                            result, Collector.database_name + "-" + db_date + ".csv"
                        )
                    else:
                        Collector.__append_to_csv(
                            result,
                            Collector.database_name + "-" + db_date + "_test" + ".csv",
                        )

        Collector.__print(
            "Finished "
            + str(tot_horoscopes)
            + " horoscopes in "
            + str(datetime.now() - starting_time)
        )

    @staticmethod
    def get_horoscope_thread(date, sign):
        try:
            return Collector.__get_horoscope(date, sign)
        except FileNotFoundError as e:
            print(e)

    @staticmethod
    def __print(string):
        time = datetime.now().strftime("%H:%M")
        print(time + " [Collector]: " + string)

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
        with open(filename, "a") as csvfile:
            fieldnames = ["horoscope", "sign", "month", "day"]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            if not filexists:
                writer.writeheader()
            writer.writerow(
                {
                    "horoscope": horoscope["horoscope"],
                    "sign": horoscope["sign"],
                    "month": horoscope["date"].strftime("%m"),
                    "day": horoscope["date"].strftime("%d"),
                }
            )

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
            raise TypeError(
                "Wrong sign type, expected str or int, got " + str(type(sign)) + "."
            )

        url = (
            "https://www.horoscope.com/us/horoscopes/general/horoscope-archive.aspx?sign="
            + sign_n
            + "&laDate="
            + date_string
        )
        response = requests.get(url)
        tree = html.fromstring(response.content)

        # Extract elements from html
        horoscope_text = "".join(
            tree.xpath("//div[contains(@class, 'main-horoscope')]/p/text()")
        )
        horoscope_date = "".join(
            tree.xpath("//div[contains(@class, 'main-horoscope')]/p/strong/text()")
        )

        # Remove artifacts from text
        if horoscope_text.startswith(" - "):
            horoscope_text = horoscope_text[3:]

        # If the date did not exist on the website
        date_compare = date.strftime("%b " + Collector.day_single + ", %Y")
        if horoscope_date != date_compare:
            raise FileNotFoundError("No horoscope from {} found.".format(date_compare))
        return {
            "date": date,
            "horoscope": horoscope_text,
            "sign": sign_n,
            moon: moon.phase(date),
        }


parser = argparse.ArgumentParser(
    description="Fetches and parses data from horoscopes.com."
)
parser.add_argument(
    "-s",
    "--startdate",
    default="2017-02-21",
    help="The first date to fetch horoscopes from (default: 2017-02-21)",
)
parser.add_argument(
    "-e",
    "--enddate",
    default=datetime.now().strftime("%Y-%m-%d"),
    help="The last date to fetch horoscopes from (default:today)",
)
parser.add_argument(
    "-t",
    "--testperc",
    default=0,
    type=int,
    help="The percentage of the results to add to the a test database (0-100) (default:0)",
)

args = parser.parse_args()
Collector.run(
    datetime.strptime(args.startdate, "%Y-%m-%d").date(),
    datetime.strptime(args.enddate, "%Y-%m-%d").date(),
    args.testperc,
)
