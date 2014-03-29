"""
Simple Sitemap Generator for Python 3.x
Uses Breadth-first search algorithm (BFS) and BeautifulSoup library

Crawls whole website to find all links
Finds and marks not valid links
Saves all fetched data in Database
"""

__author__ = 'mars'
__version__ = "1.2"
__email__ = "marsel.novy@gmail.com"

import config
import mysql.connector
from urllib.parse import urljoin
from urllib.parse import urlparse
from urllib.request import urlopen
from mysql.connector import errorcode
from bs4 import BeautifulSoup
from collections import deque
from datetime import datetime


queue = deque([config.site])  # Adding url to the queue for processing
site = urlparse(config.site).netloc
table_name = site.replace('.', '_')
host = 1

try:
    cnx = mysql.connector.connect(**config.db)
    c = cnx.cursor()

    try:
        c.execute("""DROP TABLE IF EXISTS %s""" % table_name)

    except mysql.connector.Error as err:

        if err.errno != errorcode.ER_BAD_TABLE_ERROR:
            raise

    c.execute("""CREATE TABLE %s (
           id         int         NOT NULL AUTO_INCREMENT PRIMARY KEY,
           host       int         NOT NULL,
           link       char(255)   NOT NULL UNIQUE KEY,
           name       char(255)   NOT NULL,
           checked     char(32)    DEFAULT  '--')""" % table_name)  # 'host' refers to a page id were link was found

    c.execute("""INSERT INTO %s (host, link, name) VALUES (%%s, %%s, 'Start page')""" % table_name, (0, config.site))

    while True:

        if queue[0].find(site) != -1:

            try:
                page = urlopen(queue[0])

            except Exception as err:   # Marking link as bad if it wasn't processed properly

                c.execute("""UPDATE %s SET checked = '%s' WHERE link = '%s'""" % (table_name, err, queue[0]))

            else:  # Working links are also adding to the queue as pages for further processing

                c.execute(
                    """UPDATE %s SET checked = '%s' WHERE link = '%s'""" % (table_name, str(datetime.now()), queue[0]))
                parsed_html = BeautifulSoup(page)

                for link in parsed_html.find_all('a'):  # Obtaining all links from current page

                    result = urljoin(queue[0], link.get('href'))  # Absolute path of a link
                    c.execute("""SELECT link FROM %s WHERE link = %%s""", table_name, (result,))

                    if c.rowcount < 1:
                        c.execute("""INSERT IGNORE INTO %s (host, link, name) VALUES (%%s, %%s, %%s)""" % table_name,
                                  (host, result, str(link.string),))

                    queue.append(result)

            cnx.commit()

        queue.popleft()

        if config.number_of_pages <= host or len(queue) == 0:
            print("Work's done. Number of processed pages = %s " % host)
            break

        host += 1

except mysql.connector.Error as err:

    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print("Something is wrong with your user name or password")

    elif err.errno == errorcode.ER_BAD_DB_ERROR:
        print("Database does not exists")

    else:
        print(err)
else:
    cnx.close()
