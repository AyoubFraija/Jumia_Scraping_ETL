import requests
import pandas as pd 
from bs4 import BeautifulSoup
import mysql.connector
from sqlalchemy import create_engine

#Setting the URL and the keywords
URL="https://www.jumia.ma/catalog/?q="
MOT_CLE=["gaming","cosmetic","vetement","maison","jouet"]
HEADER=({'User-Agent':'Enter your User-Agent','Accept-Language':'fr-CH, fr;q=0.9'})
nom=[]
prix=[]
note=[]
vendeur=[]
produit=[]
categorie=[]

#Function to get the name of the product  
def get_title(new_soup):
    nom=new_soup.find('h1',attrs={'class':'-fs20 -pts -pbxs'})
    if nom is None:
        return None
    else :
        return nom.text
    
#Function to get the price of the product
def get_prix(new_soup):
    prix=new_soup.find('span',attrs={'class':'-b -ltr -tal -fs24 -prxs'})
    if prix is None:
        return None
    else :
        return prix.text
    
#Function to get the rating of the product
def get_rating(new_soup):
    note=new_soup.find('div',attrs={'class':'-fs29 -yl5 -pvxs'})
    if note is None:
        return None
    else :
        return note.text
    
#Function to get the seller of the product
def get_seller(new_soup):
    vendeur=new_soup.find_all('p',attrs={'class':'-m'})
    return vendeur[0].text

#Extraction 
def extraction():
    for category in MOT_CLE:
       for i in range (1,5):
          #Sending a request to the website
           website=requests.get(URL+category+"&page="+str(i)+"#catalog-listing",headers=HEADER)
           #Transformation into html
           soup = BeautifulSoup(website.content,"html.parser")
           #Finding the links of the products's pages
           liens=soup.find_all('a',attrs={'class':'core'})
           for j in range(0,30):
                liens_http="https://www.jumia.ma/"+liens[j].get('href')
                product_page=requests.get(liens_http,headers=HEADER)
                new_soup=BeautifulSoup(product_page.content,"html.parser")
                nom.append(get_title(new_soup))
                prix.append(get_prix(new_soup))
                note.append(get_rating(new_soup))
                vendeur.append(get_seller(new_soup))
                categorie.append(category)
    # Creating a dataframe
    Produit={
        "nom" : nom ,
        'prix' : prix ,
        'vendeur' : vendeur ,
        'note' : note,
        'categorie' : categorie
    }
    donnée=pd.DataFrame.from_dict(Produit)
    return donnée

#Transformation
def transform(**kwargs)  :
    donnée = kwargs['ti'].xcom_pull(task_ids='extract')
    donnée = donnée[donnée['nom'].str.len() <= 255]
    #Droping the rows with missing rating values
    donnée.drop(donnée[donnée['note'].isnull()].index,inplace=True)
    #Droping the rows with missing price values
    donnée.drop(donnée[donnée['prix'].str.contains("-")].index,inplace=True)
    #Transforming the price and the rating into float
    donnée['prix'] = donnée['prix'].str.replace('Dhs', '').str.replace(',', '').str.strip().astype(float)
    donnée['note']=donnée['note'].str.replace('/5','').astype(float)
    return donnée

#Loading
def loading_db(**kwargs) :
    donnée = kwargs['ti'].xcom_pull(task_ids='transform')
    db = mysql.connector.connect(host="localhost", user="root", password="03112002", database="Produit")
    c = db.cursor()
    sql_query = "CREATE TABLE IF NOT EXISTS Produit ( Nom VARCHAR(255), Prix FLOAT, Vendeur VARCHAR(255), Note FLOAT, Categorie VARCHAR(255), PRIMARY KEY(Nom) )"
    c.execute(sql_query)
    db.commit()
    for index, row in donnée.iterrows():
        sql_check_query = "SELECT * FROM Produit WHERE Nom = %s"
        c.execute(sql_check_query, (row['nom'],))
        existing_data = c.fetchone()
        if  existing_data is None:
            sql_insert_query = "INSERT INTO Produit (Nom, Prix, Vendeur, Note, Categorie) VALUES (%s, %s, %s, %s, %s)"
            c.execute(sql_insert_query, (row['nom'], row['prix'], row['vendeur'], row['note'], row['categorie']))
            db.commit()
    db.close()
    return "Data loaded successfully"
def loading_csv(**kwargs) :
    donnée = kwargs['ti'].xcom_pull(task_ids='transform')
    donnée.to_csv('Produit.csv',index=False)
    return "Data loaded successfully"
