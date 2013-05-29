"""
Simple Sitemap Generator for Python 3.x
Uses Breadth-first search algorithm (BFS) and BeautifulSoup library

Crawls whole website to find all links
Finds and marks not valid links by adding prefix "bad link"
Saves all fetched data in Database
"""

__author__ = 'mars'
__version__ = "0.1"
__email__ = "marsel.akhmyednov@gmail.com"


import config
import re
import mysql.connector
from urllib.parse import urljoin
from mysql.connector import errorcode
from urllib.request import urlopen
from bs4 import BeautifulSoup
from collections import deque

urlopen(config.site)
name = re.compile(r'[/:.#]').sub('_', config.site)  # @var has transformed url to be stored as a table name

try:
    cnx = mysql.connector.connect(**config.db)
    c = cnx.cursor()

    try:
        c.execute("""DROP TABLE IF EXISTS %s""" % name)

    except mysql.connector.Error as err:
        if err.errno != errorcode.ER_BAD_TABLE_ERROR:
            raise

    c.execute("""CREATE TABLE %s (
           id         int         NOT NULL AUTO_INCREMENT PRIMARY KEY,
           host       int         NOT NULL,
           link       char(255)   NOT NULL)""" % name)   # Column 'host' refers to a page id were link was found

    # Adding url to the queue for processing
    queue = deque([config.site])
    host = number_of_links = 0

    while True:

        page = urlopen(queue[0])
        parsed_html = BeautifulSoup(page)

        for link in parsed_html.find_all('a'):  # Obtaining all links from current page

            result = urljoin(queue[0], link.get('href'))  # @var now has absolute path of a link
            c.execute("""SELECT id FROM %s WHERE link = %%s""" % name, (result,))

            if not c.fetchone():  # Adding new link to DB

                try:
                    urlopen(result)

                except BaseException:   # Marking link as bad if it wasn't processed properly
                    result = 'bad link: ' + result

                else:  # Working links are also adding to the queue as pages for further processing
                    queue.append(result)

                c.execute("""INSERT IGNORE INTO %s (host, link) VALUES (%%s, %%s)""" % name,
                          (host, result))
                cnx.commit()

            number_of_links += 1

        # All links from the first page in the queue[] fetched
        queue.popleft()

        if number_of_links > config.number_of_links or len(queue) == 0:
            print("Work's done. Number of links = %s " % number_of_links)
            break

        c.execute("""SELECT id FROM %s WHERE link = %%s""" % name, (queue[0],))
        host = c.fetchone()[0]

except mysql.connector.Error as err:

    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print("Something is wrong with your user name or password")

    elif err.errno == errorcode.ER_BAD_DB_ERROR:
        print("Database does not exists")

    else:
        print(err)
else:
    cnx.close()
