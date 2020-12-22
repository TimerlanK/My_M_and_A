import pandas as pd
import re
import numpy as np
import sqlite3

def clean_gender(df):
    df['gender'] = df['gender'].str.replace('0','male')
    df['gender'] = df['gender'].str.replace('1','female')
    df['gender'] = df['gender'].str.replace(r'\bF\b','female',regex = True)
    df['gender'] = df['gender'].str.replace(r'\bM\b','male',regex = True)

def clean_firstname(df):
    df['firstname'] = df['firstname'].str.replace(r'\"','',regex = True)
    df['firstname'] = df['firstname'].str.replace(r"\\",'',regex = True)

def clean_lastname(df):
    df['lastname'] = df['lastname'].str.replace(r'\"','',regex = True)
    df['lastname'] = df['lastname'].str.replace(r'\\','', regex = True)

def clean_email(df):
    df["email"] = df["email"].fillna('no_data')
    df["email"] = df["email"].str.replace('forgottoask@woodinc','no_data')

def clean_city(df):
    df["city"] = df["city"].str.replace(r'\W','_', regex = True)

def country_usa(df):
    df['country'] = 'USA'

def eliminate_prefix(df):
    for column_name in df.columns:
        df[column_name] = df[column_name].str.replace(r'string_','',regex = True)
        df[column_name] = df[column_name].str.replace(r'integer_','',regex = True)
        df[column_name] = df[column_name].str.replace(r'boolean_','',regex = True)
        df[column_name] = df[column_name].str.replace(r'character_','',regex = True)

def add_created_at(df):
    df['created_at'] = df['email']
    df["created_at"] = df["created_at"].str.split('@').str[1]
    df["created_at"] = df["created_at"].fillna('no_data')

def df_all_lowercase(df):
    for column in df.columns:
        try:
            df[column]=df[column].str.lower()
        except:
            continue

def split_fullname(df):
    name_surname_list = df["fullname"].str.split(' ',n = 1,expand=True)
    df["firstname"] = name_surname_list[0]
    df['lastname'] = name_surname_list[1]
    df.drop(columns=["fullname"],inplace=True)

def clean_age(df):
    df['age'] = df["age"].str.replace(r'[a-zA-Z]+','',regex=True)

def column_order(df):
    # print(df.columns)
    df = df[['gender','firstname','lastname','email','age','city','country','created_at','referral']]
    return df

def clean_append_3csv(file1,file2,file3):
    df_1 = pd.read_csv(file1)
    df_2 = pd.read_csv(file2,sep=';',header = None,names = ['age','city','gender','fullname','email'])
    df_3 = pd.read_csv(file3,skiprows=1,header = None,sep='\t',names = ['gender','fullname','email','age','city','country'])
    #column formatting
    df_1 = df_1.rename(str.lower,axis='columns')
    #add referral and created_at columns to df_1
    df_1.rename(columns={"username":"referral"},inplace=True)
    add_created_at(df_1)
    #add referral and created_at columns to df_2
    df_2['referral'] = 'no_data'
    add_created_at(df_2)
    #add referral and created_at columns to df_3
    df_3['referral'] = 'no_data'
    add_created_at(df_3)
    #clean df_1
    clean_gender(df_1)
    clean_firstname(df_1)
    clean_lastname(df_1)
    clean_email(df_1)
    clean_city(df_1)
    country_usa(df_1)
    df_all_lowercase(df_1)
    #column order
    df_1 = column_order(df_1)
    #clean df_2
    clean_gender(df_2)
    split_fullname(df_2)
    clean_firstname(df_2)
    clean_lastname(df_2)
    clean_email(df_2)
    clean_city(df_2)
    country_usa(df_2)
    clean_age(df_2)
    df_all_lowercase(df_2)
    df_2 = column_order(df_2)
    #clean df_3
    eliminate_prefix(df_3)
    clean_gender(df_3)
    split_fullname(df_3)
    clean_firstname(df_3)
    clean_lastname(df_3)
    clean_email(df_3)
    clean_city(df_3)
    country_usa(df_3)
    clean_age(df_3)
    df_all_lowercase(df_3)
    df_3 = column_order(df_3)
    #append Dataframes
    df_1 = df_1.append(df_2,ignore_index=True)
    df_1 = df_1.append(df_3,ignore_index=True)

    # see the datafrime
    # pd.set_option('max_columns', None) 
    # pd.set_option('max_rows', None) 
    # print(df_1.head())
    # print(df_2.head())
    # print(df_3.head())
    return df_1

def convert_csv_to_db(df,db,table):
    column_list = list(df.columns)
    try:
        conn = sqlite3.connect(db)
        print("database created")
    except Exception as e:
        print("Error in database creation", str(e))

    cur = conn.cursor()
    cur.execute('DROP TABLE IF EXISTS '+table)
    query_1 = 'CREATE TABLE ' + table + '({0})'
    query_1 = query_1.format(','.join(column_list))
    cur.execute(query_1)
    query_2 = 'INSERT INTO ' + table + "({0}) VALUES ({1})"
    query_2 = query_2.format(','.join(column_list),','.join('?'*len(column_list)))
    for index,row in df.iterrows():
        cur.execute(query_2,row)
    conn.commit()
    conn.close()

def db_to_sql(db_file,sql_file):
    con = sqlite3.connect(db_file)
    with open(sql_file, 'w') as f:
        for line in con.iterdump():
            f.write('%s\n' % line)
    con.close()

def m_and_a(file_1,file_2,file_3,sql_name):
    result_df = clean_append_3csv(file_1,file_2,file_3)    
    db_name = 'no_wood.db'
    table_name = 'customers'
    convert_csv_to_db(result_df,db_name,table_name)
    db_to_sql(db_name,sql_name)




#MAIN 
file1 = 'only_wood_customer_us_1.csv'
file2 = 'only_wood_customer_us_2.csv'
file3 = 'only_wood_customer_us_3.csv'
sql_name = 'plastic_free_boutique.sql'

m_and_a(file1,file2,file3,sql_name)






