#Python-Tool zum laden der Twitter-Daten

from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener

#consumer key, consumer secret, access token, access secret.
ckey=""
csecret=""
atoken=""
asecret=""

GEOBOX_GER = [5.0770049095, 47.2982950435, 15.0403900146, 54.9039819757]
blabla = ['en', 'de']

fobj = open("twitter.json","a")

class listener(StreamListener):

    def on_data(self, data):
        print(data)
        fobj.write(str(data))
        return(True)

    def on_error(self, status):
        print status

auth = OAuthHandler(ckey, csecret)
auth.set_access_token(atoken, asecret)

twitterStream = Stream(auth, listener())
twitterStream.filter(locations = GEOBOX_GER, languages = blabla)

fobj.close()

