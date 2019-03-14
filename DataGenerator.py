import numpy as np
import pandas as pd
import random
import string
import os

os.dir = '/Users/york/Desktop'

def random_character(n, upper_l = 20,lower_l = 10,char = string.ascii_lowercase):
    list_1 = []
    a = np.random.randint(lower_l,upper_l,n)
    for i in a:
        list_1.append((''.join(random.choice(char) for x in range(i))))
    return list_1

def MyPage(n):
    ID = np.asarray([i for i in range(1,(n+1))])
    Name = random_character(n, upper_l = 20,lower_l = 10,char = string.ascii_lowercase)
    Nationality = random_character(n, upper_l = 20,lower_l = 10,char = string.ascii_lowercase)
    CountryCode = np.asarray(np.random.randint(1,70,n))
    Hobby = random_character(n, upper_l = 20,lower_l = 10,char = string.ascii_lowercase)
    MyPage = (np.vstack((ID,Name,Nationality,CountryCode,Hobby))).T
    return MyPage
def Friends(n,num_high,num_low,ID_low,ID_high):
    FriendRel =[i for i in range(num_low,(num_high+1))]
    PersonID=[]
    MyFriend=[]
    for i in range(n):
        ID = np.random.randint(ID_low,ID_high)
        MyF = np.random.randint(ID_low,ID_high)
        while ID == MyF:
            ID = np.random.randint(ID_low,ID_high)
        PersonID.append(ID)
        MyFriend.append(MyF)
    DateofFriendship = np.ndarray.tolist(np.random.randint(1,1000000,n))
    Desc = random_character(n, upper_l = 50,lower_l = 20,char = string.ascii_lowercase)
    Friends = (np.vstack((FriendRel,PersonID,MyFriend,DateofFriendship,Desc))).T
    return Friends
def AccessLog(n,num_high,num_low,ID_low,ID_high):
    AccessID = [i for i in range(num_low,(num_high+1))]
    ByWho=[]
    WhatPage=[]
    for i in range(n):
        ID = np.random.randint(ID_low,ID_high)
        MyF = np.random.randint(ID_low,ID_high)
        while ID == MyF:
            ID = np.random.randint(ID_low,ID_high)
        ByWho.append(ID)
        WhatPage.append(MyF)
    TypeOfAccess = random_character(n, upper_l = 50,lower_l = 20,char = string.ascii_lowercase)
    AccessTime = np.ndarray.tolist(np.random.randint(1,1000000,n))
    AccessLog = (np.vstack((AccessID,ByWho,WhatPage,TypeOfAccess,AccessTime))).T
    return AccessLog


#create MyPage
a =MyPage(100000)
df = pd.DataFrame(data=a)
df.to_csv('MyPage.csv',index=False,header=False,mode='a+')

#create AccessLog
num_low = 1
num_high = 100000
ID_low = 1
ID_high = 1000
for j in range(100):
    a = AccessLog(100000,num_high,num_low,ID_low,ID_high)
    num_low = num_high+1
    num_high = num_high+100000
    ID_low = ID_high+1
    ID_high = ID_high+1000
    df = pd.DataFrame(data=a)
    df.to_csv('AccessLog.csv',index=False,header=False,mode='a+')
#create Friends
num_low = 1
num_high = 100000
ID_low = 1
ID_high = 500
for j in range(200):
    a = Friends(100000,num_high,num_low,ID_low,ID_high)
    num_low = num_high+1
    num_high = num_high+100000
    ID_low = ID_high+1
    ID_high = ID_high+500
    df = pd.DataFrame(data=a)
    df.to_csv('Friends.csv',index=False,header=False,mode='a+')


