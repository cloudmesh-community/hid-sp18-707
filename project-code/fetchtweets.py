####
#
# Twitter API querying, adapted from Joel Grus
# https://github.com/joelgrus/data-science-from-scratch/blob/master/code-python3/getting_data.py
#
####
import json
import sys
import time

from twython import TwythonStreamer

CONSUMER_KEY = 'key here'
CONSUMER_SECRET = 'secret here'
ACCESS_TOKEN = 'access token here'
ACCESS_SECRET = 'access secret here'
MAX_TWEETS = 1000
OUTPUT = 'obamcare2018test'.format(int(time.time()))


class MyStreamer(TwythonStreamer):

    count = 0

    def on_success(self, data):

        if data['lang'] == 'en':
            print(data)
            with open(OUTPUT, 'a') as f:
                f.write(json.dumps(data) + '\n')
            self.count += 1

        if self.count >= MAX_TWEETS:
            self.disconnect()

    def on_error(self, status_code, data):
        print(status_code, data)
        self.disconnect()


if __name__ == "__main__":
    keywords_and = ' '.join(sys.argv[1:])

    stream = MyStreamer(CONSUMER_KEY, CONSUMER_SECRET,
                        ACCESS_TOKEN, ACCESS_SECRET)

    stream.statuses.filter(track=['obamacare', 'ACA', 'affordablecareact', 'affordable care act'])