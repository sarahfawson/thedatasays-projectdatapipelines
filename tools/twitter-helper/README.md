# Twitter Helper Instructions

### setup steps
1. Create a python virtualenv
2. Install all the packages in the requirements.txt file (just run `pip install -r requirements.txt`)
3. Fill out the config.json file in the etc folder with your twitter api credentials and the twitter accounts you want to follow

### How to use it
1. Activate your twitter virtualenv by making sure you first navigate to the twitter-helper folder, then run the command `source TwitterHelperEnv/bin/activate`
2. Ensure your config.json file has the accounts you want to pull data for in it (don't forget the @symbol!)
3. run the command `python twitter-helper.py`
4. If for some reason an account shows up with no tweets, just rerun the script but only include that one account in the config.json file