"""
MIT License

Copyright (c) 2020 Gabriele Pisciotta, Miriana Somenzi, Elisa Barisani, Giulio Rossetti

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""


import csv
from telethon import TelegramClient, sync
import pandas as pd
import numpy as np
import os
from pathlib import Path

#####################
# SCRAPING
#####################

# in order to scrape telegram's messages:
# - register yourself
# - create an app on https://my.telegram.org/apps
# - write here api_id and api_hash (change these default values!!)
api_id = 0
api_hash = ""

# Insert here the group ids and max number of messages to scrape:
# for example, if you want to dump all the messages, you can just take the last message's url and use that number here:
groups = [
          ('Group1', 123123),
          ('Group2', 123124)
          ]


# You'll be asked to insert your mobile phone number and the OTP
client = TelegramClient('session', api_id, api_hash).start()

for group_username, max in groups:
    print("[+] Scraping group: {}".format(group_username))
    
    # This is for writing the header of the dataset in the file
    keys = ['message_id', 'sender_id', 'reply_to_msg_id', 'time']
    
    # Create directory if not exists
    Path(os.path.join('..', 'data', group_username)).mkdir(parents=True, exist_ok=True)
    
    file_csv = os.path.join('..', 'data', group_username, '{}_messages.csv'.format(group_username))
    with open(file_csv, 'w') as output_file:
        dict_writer = csv.DictWriter(output_file, keys)
        dict_writer.writeheader()

    # We scrape starting from the first message and until we haven't reached the max number of messages.
    # For each message, we create a dictionary associated to its most important info for us and then we append it to
    # the "messages" list. After that, we save the list to a file in CSV format.
    #
    # This could take up to 3 hours for each group... 
    messages = []
    chats = client.get_messages(group_username,
                                max_id=max,
                                min_id=0,
                                reverse=True)

    if len(chats):

        for chat in chats:
            row = {
                    'message_id'     : chat.id,
                    'sender_id'      : chat.from_id,
                    'reply_to_msg_id': chat.reply_to_msg_id,
                    'time'           : chat.date
                    }
            messages.append(row)

        if len(messages):
            with open(file_csv.format(group_username), 'a') as output_file:
                dict_writer = csv.DictWriter(output_file, keys)
                dict_writer.writerows(messages)

print("[+] (OK) Scraping finished!")



#####################
# PREPROCESSING
#####################

# Open a file to write groups stats
group_info = open(os.path.join('..', 'data', 'groups_info.csv'), 'w')
group_info.write("Group;Users;Messages;Replies;Percentage;RepliesAfterCleaning,PercentageAfterCleaning\n")

for group, _ in groups:
    print("[+] Preprocessing group: {}".format(group))
    file_csv = os.path.join('..', 'data', group, '{}_messages.csv'.format(group))
    cleaned_csv = os.path.join('..', 'data', group, '{}_cleaned_messages.csv'.format(group))

    df = pd.read_csv(file_csv, usecols=['message_id', 'sender_id', 'reply_to_msg_id', 'time'])
    
    # Some infos about the group
    messages = len(df)
    users = len(df['sender_id'].unique())
    replies = df.reply_to_msg_id.count()
    percentage_of_reply = replies * 100 / messages

    # Replace replace reply_to_msg_id with reply_to_user
    df.index = df['message_id']
    for i, row in df.iterrows():
        if row['reply_to_msg_id'] > 0 and pd.isna(row['reply_to_msg_id']) == False:
            msg_index = int(row['reply_to_msg_id'])
            try:
                reply_to_user = int(df.at[msg_index, 'sender_id'])

                # Avoid to save auto-replies
                if reply_to_user == int(row['sender_id']):
                    df.at[i, 'reply_to_msg_id'] = np.nan
                else:
                    df.at[i, 'reply_to_msg_id'] = reply_to_user

            # If reply_to_msg_id refers to a deleted message, it
            # will raise an exception. So instead of considering
            # the user id associated to a deleted message (which we can't retrieve)
            # we just put a NaN there.
            except KeyError:
                df.at[i, 'reply_to_msg_id'] = np.nan
            except ValueError:
                df.at[i, 'reply_to_msg_id'] = np.nan

    # So after that, we change the name of the column and save the new dataset to another file
    df.rename(columns={"reply_to_msg_id": "reply_to_user_id"}, inplace=True)

    # Let's drop rows with nan
    df = df.dropna()
    # Save the dataset in a new file
    df.to_csv(cleaned_csv, index=False)

    # Evaluate new infos
    replies_after_cleaning = df.reply_to_user_id.count()
    percentage_of_reply_after_cleaning = replies_after_cleaning * 100 / messages

    # Save them to file
    group_info.write("{};{};{};{};{:.2f}%;{};{:.2f}%\n".format(group,
                                                               users,
                                                               messages,
                                                               replies,
                                                               percentage_of_reply,
                                                               replies_after_cleaning,
                                                               percentage_of_reply_after_cleaning))
group_info.close()
print("[+] (OK) Preprocessing finished! Have a look at group_info.csv in order to get some useful infos about the groups.")

#####################
# EDGE LIST CREATION
#####################

dfs = []
totalsize = 0

# Merge all dataset in a single one
for group, _ in groups:
    print("[+] Adding {} to the final merged dataset...".format(group))
    cleaned_csv = os.path.join('..', 'data', group, '{}_cleaned_messages.csv'.format(group))
    df = pd.read_csv(cleaned_csv, usecols=['sender_id', 'reply_to_user_id'])
    df.sender_id = df.sender_id.astype(int)
    df.reply_to_user_id = df.reply_to_user_id.astype(int)
    dfs.append(df)
    totalsize += len(df)
df = pd.concat(dfs, ignore_index=True, sort=False)

# Check if everything is added...
if len(df) == totalsize:
    print("[+] (OK) The size of the final dataframe is the sum of the single ones.")

# Set the number of replies as weight
df = df.groupby(df.columns.tolist()).size().reset_index().rename(columns={0:'records'})
df.rename(columns={"sender_id": "source", "reply_to_user_id": "target", "records": "weight"}, inplace=True)

# Save the dataframe (edges list)
graph_file = os.path.join('..', 'data', 'graph.csv'.format(group))
df.to_csv(graph_file, index=False)
print("[+] Everything finished!")

