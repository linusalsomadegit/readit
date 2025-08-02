import requests
import shutil
import yt_dlp
import queue
import json
import csv
import re
import os
from dotenv import load_dotenv
from pathlib import Path
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Float, Text
from sqlalchemy.orm import sessionmaker
from collections import Counter
from urllib.parse import urlparse, parse_qs, urljoin

import traceback
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import time
from bs4 import BeautifulSoup

def clear():
            os.system('cls' if os.name == 'nt' else 'clear')

def get_width():
    try:
        return shutil.get_terminal_size().columns
    except:
        return 80

def wide():
    ascii_big = f"""
\033[1;36m ____                            \033[1;31m                  _ \033[0m
\033[1;36m/ ___|  ___ _ __ __ _ _ __   ___ \033[1;31m_   _  ___  _   _| |\033[0m
\033[1;36m\___ \ / __| '__/ _` | '_ \ / _ \\\033[1;31m | | |/ _ \| | | | |\033[0m
\033[1;36m ___) | (__| | | (_| | |_) |  __/\033[1;31m |_| | (_) | |_| |_|\033[0m
\033[1;36m|____/ \___|_|  \__,_| .__/ \___|\033[1;31m\__, |\___/ \__,_(_)\033[0m
\033[1;36m                     |_|         \033[1;31m|___/               \033[0m
    """
    print(ascii_big)

def medium():   #   use this for medium size but
                #   probably not necessary since
                #   people will either use either large or
                #   small cli
    ascii_medium = f"""
\033[1;36m ____                            \033[1;31m                  _ \033[0m
\033[1;36m/ ___|  ___ _ __ __ _ _ __   ___ \033[1;31m_   _  ___  _   _| |\033[0m
\033[1;36m\___ \ / __| '__/ _` | '_ \ / _ \\\033[1;31m | | |/ _ \| | | | |\033[0m
\033[1;36m ___) | (__| | | (_| | |_) |  __/\033[1;31m |_| | (_) | |_| |_|\033[0m
\033[1;36m|____/ \___|_|  \__,_| .__/ \___|\033[1;31m\__, |\___/ \__,_(_)\033[0m
\033[1;36m                     |_|         \033[1;31m|___/               \033[0m
    """
    print(ascii_medium)

def small():
    ascii_small = f"""
    Scrapeyou! (why is your terminal so small?!)
    """

def print_art():
    width = get_width()
    if width >= 100:
        wide()
    elif width >= 60:
        medium()
    else:
        small()


class Karma:

    def __init__(self, url):
        self.url = url
        self.rawjson = None
        self.token = self.create_token()
        if self.token == None:
            print("problem with token")
            exit(0)
        self.data = self.call_reddit()
        if self.data == None:
            print("problem scraping")
            exit(0)

        self.title = None
        self.author = None
        self.score = None
        self.ups = None
        self.downs = None
        self.subreddit = None
        self.tallycomments = None
        self.createdwhen = None
        self.stickied = None
        self.is_video = None
        self.gilded = None
        self.awardcount = None
        self.awarders = None
        self.distinguished = None
        self.editedwhen = None
        self.commentslocked = None
        self.comments = []
        self.keywords = []
        self.userscommented = []
        self.related_posts = []

        self.get_everything()

        self.hiding = """

        self.postbody = self.get_postbody()

        self.stickied = self.get_stickied() #   should be bool?
        self.awarders = self.get_awarders()
        self.distinguished = self.get_distinguished() #     should also probably be bool
        """
        #  do searches of the subreddit for the keywords in comments and stuff,
        #   save to related posts

    def create_token(self):
        load_dotenv()
        auth = requests.auth.HTTPBasicAuth(os.getenv("REDDIT_CLIENT_ID"), os.getenv("REDDIT_CLIENT_SECRET"))
        data = {
            "grant_type": "password",
            "username": os.getenv("REDDIT_USERNAME"),
            "password": os.getenv("REDDIT_PASSWORD")
        }
        headers = {"User-Agent": "reddit-scraper/0.1"}

        res = requests.post("https://www.reddit.com/api/v1/access_token",
                            auth=auth, data=data, headers=headers)


        res_json = res.json()
        if "access_token" not in res_json:
            print("Token error:", res_json)
            return None
        return res_json["access_token"]

        token = res.json()["access_token"]

        print(token)

        return token

    def call_reddit(self):
        #   print(self.token)
        data = None

        try:
            time.sleep(1.05)
            permalink = self.url.split("reddit.com")[-1].rstrip("/")

            api_url = f"https://oauth.reddit.com{permalink}"

            headers = {
                "User-Agent": "python:reddit.scraper:v1.0 (by u/Dapper-Hunt-4390)",
                "Authorization": f"bearer {self.token}"
            }

            resp = requests.get(api_url, headers=headers)
            resp.raise_for_status()
            this_post = resp.json()
            self.rawjson = this_post

            data = this_post[0]["data"]["children"][0]["data"]
            return data

        except Exception as e:
            print(f"error scraping this post, skipping: {e}")

    def get_everything(self):
        self.title = self.data.get("title")
        self.author = self.data.get("author")
        self.score = self.data.get("score")
        self.ups = self.data.get("ups")
        self.downs = self.data.get("downs")
        self.subreddit = self.data.get("subreddit")
        self.tallycomments = self.data.get("num_comments")
        self.createdwhen = self.data.get("created_utc")
        self.stickied = self.data.get("stickied")
        self.is_video = self.data.get("is_video") #     might just be useless bool, but keep for now
        self.gilded = self.data.get("gilded")
        self.awardcount = self.data.get("total_awards_received")
        self.awarders = self.data.get("awarders")
        self.distinguished = self.data.get("distinguished")
        self.editedwhen = self.data.get("edited")
        self.commentslocked = self.data.get("locked")
        self.userscommented = None
        #   self.comments = None
        self.get_keywords()
        self.get_related_posts()

    def get_keywords(self):
        keywords = []
        users = []
        for comment in self.rawjson[1]["data"]["children"]:
            if comment["kind"] == "t1":  # regular comment
                body = comment["data"].get("body")
                author = comment["data"].get("author")
                if body:
                    self.comments.append(body)
                    # simple keyword extraction (split words)
                    for word in re.findall(r"\w+", body.lower()):
                        if len(word) > 4:  # avoid trivial words
                            keywords.append(word)
                if author and author != "[deleted]":
                    users.append(author)

        keywords = list(set(keywords))[:20]   #     instead of magic number maybe put in a name variable to pass dynamically
        users = list(set(users))[:20]

        self.keywords = keywords
        self.userscommented = users

    def get_related_posts(self):
        if not self.keywords:
            return []

        subreddit = self.subreddit
        headers = {
            "User-Agent": "python:reddit.scraper:v1.0 (by u/Dapper-Hunt-4390)",
            "Authorization": f"bearer {self.token}"
        }

        related = []

        for kw in self.keywords:
            search_url = f"https://oauth.reddit.com/r/{subreddit}/search?q={kw}&restrict_sr=1&sort=top&limit=1"
            try:
                time.sleep(1.05)
                r = requests.get(search_url, headers=headers)
                r.raise_for_status()
                sdata = r.json()
                posts = sdata.get("data", {}).get("children", [])
                if posts:
                    top_post = posts[0]["data"].get("permalink")
                    if top_post:
                        full_url = "https://www.reddit.com" + top_post
                        related.append(full_url)
            except Exception as e:
                print(f"[get_related_posts] error fetching with kw='{kw}': {e}")
                continue

        self.related_posts = related
        return related


class Spider:

    def __init__(self, url, parsedorders, engine):
        #   clear()
        self.engine = engine
        self.qdposts = queue.Queue()
        self.parsedorders = parsedorders

        if url is not None:
            self.qdposts.put(url)
        self.tally = 0
        self.seen = set()
        self.stopat = parsedorders.scrape_limit
        if self.tally == 0:
            self.loadlastqueue()
        if self.qdposts.empty() and self.parsedorders.uselastscrape:
            Help().emptylistgiven()
        self.recurse()

    def recurse(self):
        #   gonna export the data here somehow, probably in Out class, but
        #   for now just print
        while not self.qdposts.empty() and self.tally < self.stopat:
            current_url = self.qdposts.get()
            if current_url in self.seen:
                continue
            self.seen.add(current_url)

            try:
                post = Karma(current_url)

                self.tally += 1

                Rambling(post, self.tally, self.parsedorders, self.stopat, self.qdposts, self.engine, self.seen)

                for new_url in post.related_posts or []:
                    clean_url = new_url.split("?")[0]
                    if clean_url not in self.seen:
                        self.qdposts.put(clean_url)        #   how many comments/recs is dynamic based on
            except Exception as e:                                      #   what is relevant and found, see todo doc
                #   if self.tally > 0:
                print(f"error scraping {current_url}: {e}")

    def loadlastqueue(self, filename="rollingurls.csv"):
        if self.parsedorders.uselastscrape:                 # and self.qdposts == None:
            with open(filename, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line.startswith("http"):
                        self.qdposts.put(line)

            with open(filename, 'w', encoding='utf-8') as f:
                pass
            return self.qdposts


class Rambling:

    def __init__(self, this_post, tally, parsedorders, stopat, qdposts, engine, seen):
        self.parsedorders = parsedorders
        self.stopat = stopat
        self.engine = engine
        self.post1 = this_post
        self.tally = tally
        self.qdposts = qdposts
        self.seen = seen

        if self.parsedorders.printcount:
            clear()
            print(f"post # in search: \033[1;36m{self.tally}\033[0m")

        if self.parsedorders.printbasic:
            self.printbasic()

        if self.parsedorders.printextra:
            self.printextra()

        if self.parsedorders.csvout:
            self.export_csv()

        if self.parsedorders.tursoout:
            self.export_turso()

        if self.stopat == self.tally:
            self.scrape_finished()

    def printbasic(self):

        print(f"\nurl: {self.post1.url}")
        print(f"\ntitle: {self.post1.title}")
        print(f"\nauthor: {self.post1.author}")
        print(f"\nscore: {self.post1.score}")
        print(f"\nupvotes: {self.post1.ups}")
        print(f"\ndownvotes: {self.post1.downs}")
        print(f"\nsubreddit: {self.post1.subreddit}")
        print(f"\nnumber of comments: {self.post1.tallycomments}")
        print(f"\nupload timestamp: {self.post1.createdwhen}")



    def printextra(self):

        print(f"\nis stickied: {self.post1.stickied}")
        print(f"\nis video: {self.post1.is_video}")
        print(f"\nis gilded: {self.post1.gilded}")
        print(f"\ntotal awards: {self.post1.awardcount}")
        print(f"\nawarders: {self.post1.awarders}")
        print(f"\ndistinguished: {self.post1.distinguished}")
        print(f"\ntime edited: {self.post1.editedwhen}")
        print(f"\ncomments locked: {self.post1.commentslocked}")
        print(f"\nusers commented: {self.post1.userscommented}")
        print(f"\ncomments: {self.post1.comments}")
        print(f"\nkeywords: {self.post1.keywords}")
        print(f"\nrelated posts: {self.post1.related_posts}")
        exit(0)

    def export_csv(self, filename="output.csv"):

        with open(filename, mode='a', newline='', encoding='utf-8') as csvfile:
            #   writer.writerow(self.post1.__dict__.values()) this is ludicrous it could
            #   be cool but we'll see, based on flags
            if self.post1 is not None:
                writer = csv.writer(csvfile)
                if self.parsedorders.savebasic:
                    writer.writerow([
                        self.tally,

                    ])

                if self.parsedorders.saveextra:
                    writer.writerow([
                        self.post1.comments,

                    ])

                if self.tally == 0:
                    if self.parsedorders.savebasic:
                        basic = """posteo_number, url, title, uploader, views, duration, dateup, likes, dislikes, keywords, """
                        writer.writerow(basic)
                    if self.parsedorders.saveextra:
                        extra = """comments, description, hashtags, startheat, endheat, peakheat, related_posts, """

    def scrape_finished(self):
        #   clear()
        print(f"\n")
        print(f"scrape complete!")
        print(f"\n")
        print(f"thank you for using")
        print_art()
        if self.parsedorders.savethisscrape == True:
            self.save_urls()
        exit(0)

    def export_turso(self):
        if parsedorders.savebasic:
            try:
                values = {
                    'post_number': self.tally + 1,
                    'url': self.post1.url,
                    'title': self.post1.title,
                    'uploader': self.post1.uploader,
                    'views': self.post1.views,
                    'duration': self.post1.duration,
                    'dateup': self.post1.dateup,
                    'likes': self.post1.likes,
                    'dislikes': self.post1.dislikes,
                    'keywords': ", ".join(self.post1.keywords) if self.post1.keywords else None,
                }

                with self.engine.begin() as conn:
                    conn.execute(posteo_table.insert().values(**values))

            except Exception as e:
                if self.parsedorders.printbasic or self.parsedorders.printextra or self.parsedorders.printcount:
                    print(f"\033[1;31mWARNING\033[0m: error exporting current post to tursodb: {e}")

        if parsedorders.saveextra:
            try:
                values = {
                    'comments': "\n".join(self.post1.comments) if self.post1.comments else None,
                    'description': self.post1.description,
                    'hashtags': ", ".join(self.post1.hashtags) if self.post1.hashtags else None,
                    'startheat': self.post1.startheat,
                    'endheat': self.post1.endheat,
                    'peakheat': self.post1.peakheat,
                    'related_posts': ", ".join(self.post1.related_posts) if self.post1.related_posts else None
                }

                with self.engine.begin() as conn:
                    conn.execute(posteo_table.insert().values(**values))

            except Exception as e:
                if self.parsedorders.printbasic or self.parsedorders.printextra or self.parsedorders.printcount:
                    print(f"\033[1;31mWARNING\033[0m: error exporting current post to tursodb: {e}")

    def save_urls(self, filename="rollingurls.csv"):
        with open(filename, mode='w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            for url in self.seen:
                writer.writerow([url])



class Flags():
    #   have numerical flag (-5) for scrape specificity, dynamic if blank
    def __init__(self, orders):
        self.orders = orders
        pieces = self.parse_user_input()
        self.url = pieces['url']
        self.rawflags = pieces['flags']
        self.scrape_limit = pieces['scrape_limit']
        self.printcount = True
        self.printbasic = False
        self.printextra = False
        self.savebasic = True
        self.saveextra = True
        self.csvout = False
        self.tursoout = False
        self.savethisscrape = True
        self.uselastscrape = False
        self.printhelp = False
        self.setupdb = False
        self.flags = self.whichflags()

    def parse_user_input(self):
        tokens = self.orders.strip().split()

        #   re to get url
        url_pattern = re.compile(r"(https?://)?(www\.)?reddit\.com/r/\w+/comments/\w+/\S+")

        url = None
        flags = set()
        limit_flag = None

        for token in tokens:
            #   get url
            if url_pattern.match(token):
                url = token
            #   get flags
            elif re.match(r"^-{1,2}[a-zA-Z]+$", token):
               flags.add(token)
            elif re.match(r"^-\d+$", token):  # e.g., -100
                limit_flag = abs(int(token))

        return {
            "url": url,
            "flags": flags,
            "scrape_limit": limit_flag
        }

    def whichflags(self):
        if self.rawflags == None:
            return

        valid_flags = {'-h', '--help', '-q', '--quiet', '-b', '-e', '-B', '-E', '--csv', '-c', '-X', '-u', '-t'}
        unknown_flags = {
            flag for flag in self.rawflags
            if flag not in valid_flags and not re.match(r"^-\d+$", flag)
            }

        if '-h' in self.rawflags:
            self.printhelp = True
            return
        if '--help' in self.rawflags:
            self.printhelp = True
            return
        if '-q' in self.rawflags:
            self.printbasic = self.printextra = self.printcount = False
        if '--quiet' in self.rawflags:
            self.printbasic = self.printextra = self.printcount = False
        if '-s' in self.rawflags:
            self.setupdb = True
        if '--setup' in self.rawflags:
            self.setupdb = True
        if '-b' in self.rawflags:
            self.printbasic = True
        if '-e' in self.rawflags:
            self.printextra = True
        if '-B' in self.rawflags:
            self.savebasic = False
        if '-E' in self.rawflags:
            self.saveextra = False
        if '--csv' in self.rawflags:
            self.csvout = True
        if '-c' in self.rawflags:
            self.csvout = True
        if '-X' in self.rawflags:
            self.savethisscrape = False
        if '-u' in self.rawflags:
            self.uselastscrape = True
        if '-t' in self.rawflags:
            self.tursoout = True

        if unknown_flags:
            print("\nignoring unrecognized flags:", ", ".join(unknown_flags), " beginning scrape!")


class Help():   #   print help here, then go back to entering info
    def __init__(self):
        pass

    def helppage(self):
        #   don't forget to say where the persistent option will be
        clear()
        #   \033[1;36m      cyan
        #   \033[0m         reset
        #   \033[1;31m      red
        print("""
    \033[1;36mScrape\033[1;31mYou\033[0m! [help page]


        [\033[1;36musage\033[0m]

        --help          print this message
        -h              print this message

        --quiet         do \033[1;31mNOT\033[0m print anything
        -q              do \033[1;31mNOT\033[0m print anything

        -B              do \033[1;31mNOT\033[0m save (\033[1;36mB\033[0m)ASIC data (in csv and/or turso)
        -E              do \033[1;31mNOT\033[0m save (\033[1;36mE\033[0m)XTRA data (in csv and/or turso)

        -b              DO print (\033[1;36mb\033[0m)ASIC data
        -e              DO print (\033[1;36me\033[0m)XTRA data

        --csv           DO export data to csv file
        -c              DO export data to csv file

        -t              DO export data to (\033[1;36mt\033[0m)urso db (requires .env setup)

        -X              e(\033[1;36mX\033[0m)clude current session urls (\033[1;31mwon't be saved\033[0m)
        -u              (\033[1;36mu\033[0m)se last saved session url list

        -s              (s)etup .env file (required for turso db)
        --setup         (s)etup .env file (required for turso db)


        [\033[1;36mbasic\033[0m vs \033[1;36mextra\033[0m]

        b is for BASIC data:

            url, title, uploader, views, duration,
            upload date, likes/dislikes, keywords

        e is for EXTRA data:

            comments, description, hashtags,
            heatmap, related urls


        [\033[1;36metc tips\033[0m]

        by default:

            everything is saved,
            only posteo tally is printed, terminal is kept clean for swag points!
            csv is \033[1;31mNOT\033[0m stored,
            session urls are preserved!

            quiet flags disable everything including posteo tally!
            if you're bored, print while you scrape!
            remember, BEbeq for flags!
            print is lowercase, save is uppercase, order does not matter
        """)

        choice = None
        choice = input(f"\n        press enter to start, type 'QUIT' to exit: ")

        if choice.strip().upper() == 'QUIT':
            exit(0)
        else:
            clear()
            print_art()
            main()

    def noinput(self):
        clear()
        print("""
    \033[1;31mWARNING\033[0m: no url given! did you mean -u? (use last session's urls)
        """)

        choice = None
        choice = input(f"\n        press enter to start, type 'QUIT' to exit: ")

        if choice.strip().upper() == 'QUIT':
            exit(0)
        else:
            clear()
            print_art()
            main()

    def nobothinputs(self):
        clear()
        print("""
    \033[1;31mWARNING\033[0m: previous session url list empty! verify the file or enter a new url
        """)

        choice = None
        choice = input(f"\n        press enter to start, type 'QUIT' to exit: ")

        if choice.strip().upper() == 'QUIT':
            exit(0)
        else:
            clear()
            print_art()
            main()

    def emptylistgiven(self):
        clear()
        print("""
    \033[1;31mWARNING\033[0m: url list empty! omit -u or verify rollingurls.csv is populated and in the correct place
        """)

        choice = None
        choice = input(f"\n        press enter to start, type 'QUIT' to exit: ")

        if choice.strip().upper() == 'QUIT':
            exit(0)
        else:
            clear()
            print_art()
            main()

    def twoinputs(self):
        clear()
        print("""
    \033[1;31mWARNING\033[0m: you cannot start a new scrape and use last input list! use either -u or proposte a url
        """)

        choice = None
        choice = input(f"\n        press enter to start, type 'QUIT' to exit: ")

        if choice.strip().upper() == 'QUIT':
            exit(0)
        else:
            clear()
            print_art()
            main()

    def envsetup(self):
        clear()
        print("""
    \033[1;31mWARNING\033[0m: .env does not exist!
        """)

        choice = None
        choice = input(f"\n        press enter to start, type 'SETUP' for .env setup, or type 'QUIT' to exit: ")

        if choice.strip().upper() == 'QUIT':
            exit(0)
        if choice.strip().upper() == 'SETUP':
            clear()
            db_url = input("enter your turso db url (to be stored locally): ").strip()
            clear()
            auth_token = input("enter your turso auth token (to be stored locally): ").strip()

            with open(".env", "w") as f:
                f.write(f"TURSO_DB_URL={db_url}\n")
                f.write(f"TURSO_AUTH_TOKEN={auth_token}\n")
        else:
            clear()
            print_art()
            main()


def main():
    engine = None
    if not Path(".env").exists():
        print(f"welcome! enter url and flags to get started: (-h or --help for info and -s or --setup for .env setup(\033[1;36mrecommended\033[0m))")
    else:
        print(f"welcome! enter url and flags to get started: (-h or --help for info)")
    orders = input("")
    clear()
    parsedorders = Flags(orders)
    url = parsedorders.url

    if parsedorders.printhelp:
        Help().helppage()

    if parsedorders.setupdb:
        Help().envsetup()

    if parsedorders.tursoout and not Path(".env").exists():
        Help().envsetup()

    if parsedorders.tursoout:
        load_dotenv()
        engine = create_engine(
            f"{os.getenv('TURSO_DB_URL')}?authToken={os.getenv('TURSO_AUTH_TOKEN')}",
            connect_args={"uri": True}
            )

        metadata = MetaData()

        posteo_table = Table(
            "posteos", metadata,
            Column("post_number", Integer),
            Column("url", String),
            Column("title", String),
            Column("uploader", String),
            Column("views", Integer),
            Column("duration", Float),
            Column("dateup", String),
            Column("likes", Integer),
            Column("dislikes", Integer),
            )
        metadata.create_all(engine)

    if not parsedorders.url and parsedorders.uselastscrape and not Path("rollingurls.csv").exists():
        Help().nobothinputs()

    if parsedorders.csvout:
        with open("output.csv", 'w', encoding='utf-8') as f:
            pass

    if not parsedorders.url and not parsedorders.uselastscrape:
        Help().noinput()

    if parsedorders.url and parsedorders.uselastscrape:
        Help().twoinputs()

    if parsedorders.scrape_limit == None:
        parsedorders.scrape_limit = 3

    Spider(url, parsedorders, engine)

if __name__ == "__main__":
    clear()
    print_art()
    main()
