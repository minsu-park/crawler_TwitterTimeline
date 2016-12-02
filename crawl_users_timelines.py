# encoding: UTF-8

# Requirement: http://mike.verdone.ca/twitter/

import socket
hostname = socket.gethostname()

import os
dir_base = os.path.abspath(os.path.dirname(__file__))

import sys
#sys.path = ["{}/libs/twitter-1.14.2".format(dir_base)]+sys.path
from twitter import *
import urllib2

import logging
#logging.basicConfig(filename="{}/logs/collect_users_timelines.{}.log".format(dir_base, hostname), filemode="a", level=logging.INFO, format="[ %(asctime)s ] [%(levelname)s] %(message)s")
logging.basicConfig(filename="{}/logs/collect_users_timelines.{}.log".format('/home/ubuntu/data', hostname), filemode="a", level=logging.INFO, format="[ %(asctime)s ] [%(levelname)s] %(message)s")

import time
import json
import gzip
import random
import gc

def collect_users_timelines(args):
    apikey, proxy, users_ids, worker_id = args

    # Setup proxy
#   host, port, user, password = proxy
#   os.environ["http_proxy"] = "http://{}:{}@{}:{}".format(user, password, host, port)
    
    """
    proxy_setup = urllib2.ProxyHandler(proxy)
    opener = urllib2.build_opener(proxy_setup)
    urllib2.install_opener(opener)
    """
    host, port = proxy.items()[0][0].split(':')
    os.environ["http_proxy"] = "http://{}:{}".format(host, port)

    # Setup API
#   consumer_key, consumer_secret, access_token, access_token_secret = apikey
    consumer_key, consumer_secret, access_token, access_token_secret = apikey[0].split(',')
    twitter_api = Twitter(auth=OAuth(access_token, access_token_secret, consumer_key, consumer_secret))
    logging.info("[W-{}] API Authenticated".format(worker_id))

    # Iterate over list of users
    for user_id in users_ids:

		#output_filename = "{}/data/user_timeline/{}.json.gz".format(dir_base, user_id)
        output_filename = "{}/user_timeline/{}.json.gz".format('/home/ubuntu/data', user_id)

        # Skip user if it was already collected
        if os.path.exists(output_filename):
            logging.info("[W-{}] User {} - Skipped".format(worker_id, user_id))

        # Collect all tweets of the user
        else:
            logging.info("[W-{}] User {} - Starting".format(worker_id, user_id))
            
            user_timeline = []

            # Paginate over the user timeline
            max_id = None 
            pagination_finished = False
            while not pagination_finished:
                
                # Request page
                try:
                    request_finished = False
                    while not request_finished:

                        # Request OK
                        try:
                            if max_id is None:
                                response = twitter_api.statuses.user_timeline(user_id=user_id, trim_user=True, count=200)
                            else:
                                response = twitter_api.statuses.user_timeline(user_id=user_id, trim_user=True, count=200, max_id=max_id)
                            request_finished = True

                        # API error
					#   except twitter.api.TwitterHTTPError, error:
                        except TwitterHTTPError, error:
                            
                            error_detail = json.loads(error.response_data)
                            if "error" in error_detail:
                                error_message = error_detail["error"]
                            elif "errors" in error_detail:
                                error_message = error_detail["errors"][0]["message"]
                            else:
                                error_message = "Unknown"
                            logging.warning("[W-{}] User {} - API error: {}".format(worker_id, user_id, error_message))

                            if error_message == "Rate limit exceeded":
                                response = None
                                rates = twitter_api.application.rate_limit_status(resources="statuses")
                                rate_limit_reset = rates["resources"]["statuses"]["/statuses/user_timeline"]["reset"]
                                to_sleep = max(rate_limit_reset - int(time.time()), 0)
                                logging.warning("[W-{}] User {} - Rate limit exceeded, sleeping {} seconds".format(worker_id, user_id, to_sleep+1))
                                time.sleep(to_sleep+1)
                            elif error_message == "Sorry, that page does not exist":
                                response = []
                                request_finished = True
                            elif error_message == "Not authorized.":
                                response = []
                                request_finished = True
                            elif error_message == "Could not authenticate you":
                                response = None
                                request_finished = True
                                logging.error("[W-{}] User {} - Could not authenticate: API key = {}, {}, {}, {}; Host = {}, {}, {}, {}".format(worker_id, user_id, consumer_key, consumer_secret, access_token, access_token_secret, host, port, user, password))
                            else:
                                response = None
                                request_finished = True
                                logging.error("[W-{}] User {} - Unexpected API error: {}".format(worker_id, user_id, error.response_data))

                        # Unknown error
                        except Exception, error:
                            response = None
                            request_finished = True
                            logging.error("[W-{}] User {} - Unexpected exception during request: {}".format(worker_id, user_id, str(error)))

                    # Terminate user
                    if response is None:
                        user_timeline = None
                        pagination_finished = True

                    # Append new tweets and continue to next page
                    elif len(response) > 0:
                        max_id = min([tweet["id"] for tweet in response]) - 1
                        user_timeline.extend(response)

                    # Finishes pagination
                    else:
                        pagination_finished = True

                except Exception, error:
                    user_timeline = None
                    pagination_finished = True
                    logging.error("[W-{}] User {} - Unexpected exception during pagination: {}".format(worker_id, user_id, str(error)))
                    

            # Output result only if there was no error
            if user_timeline is not None:
                #outfile = gzip.open(output_filename, "w")
                with gzip.open(output_filename, "w") as outfile:
                    json.dump(user_timeline, outfile)
                #outfile.close()
                logging.info("[W-{}] User {} - Finished ({} tweets)".format(worker_id, user_id, len(user_timeline)))
            else:
                logging.info("[W-{}] User {} - Terminated".format(worker_id, user_id))
                
            gc.collect()

    logging.info("[W-{}] done".format(worker_id))
    


if __name__ == "__main__":

    import csv
    import multiprocessing

    # Get file names from command line parameters
    if len(sys.argv) >= 4:
        filename_apikeys, filename_proxies, filename_ids = sys.argv[1:4]
    else:
        sys.exit("Usage: python {} [file name API keys] [file name proxies] [file name IDs]".format(sys.argv[0]))

    logging.info("Collector started")
    
    # Read list of API keys
    with open(filename_apikeys, "rU") as infile:
        reader = csv.reader(infile, delimiter=" ", quoting=csv.QUOTE_NONE)
        apikeys = list(reader)
    logging.info("Loaded {} API Keys".format(len(apikeys)))

    # Read list of proxies
    with open(filename_proxies, "r") as infile:
    #   reader = csv.reader(infile, delimiter=" ", quoting=csv.QUOTE_NONE)
    #   proxies = list(reader)
        proxies = json.load(infile)['proxies']
    #   print proxies['proxies'][0].items()[0][0].split(':')
    logging.info("Loaded {} proxies".format(len(proxies)))

    # Read list of users to collect
    with open(filename_ids, "r") as infile:
        all_users = [line.rstrip() for line in infile]
        random.shuffle(all_users)
    logging.info("Loaded {} users".format(len(all_users)))

    # Create chunks of users
    chunk_size = len(all_users)/len(apikeys)
    chunks_users = [all_users[i:i+chunk_size] for i in xrange(0, len(all_users)-(len(all_users) % len(apikeys)), chunk_size)]
    for i, excess_user in enumerate([user for i in xrange(len(all_users)-(len(all_users) % len(apikeys)), len(all_users), chunk_size) for user in all_users[i:i+chunk_size]]):
        chunks_users[i].append(excess_user)
    logging.info("Created {} chunks of size {}".format(len(chunks_users), chunk_size))

    # Call parallel processes
    tasks = zip(apikeys, proxies, chunks_users, xrange(1, len(apikeys)+1))
    pool = multiprocessing.Pool(processes=len(tasks))
    logging.info("Processes started")
    pool.map(collect_users_timelines, tasks)
    gc.collect()
                  
    logging.info("Collector finished")
