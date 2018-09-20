# -*- coding: utf-8 -*-
"""
Created on Wed Sep 19 10:00:40 2018

@author: charmaine
"""
'''
This is a world bank economic indicator data service.
There are mainly two parts, indicator_function and indicator api. 
'''
import pymongo,json,time
from urllib.request import urlopen
import pandas as pd
from flask import Flask,request
from flask_restplus import Resource, Api

#define api
app = Flask(__name__)
api = Api(app,
          default='World Bank Economic Indicator',
          title='World Bank Economic Indicator Data Service',
          description='This is a data service for world bank economic indicator')

#This part is the interaction from the api to database
class Indicator_Function():
    def __init__(self,uri):
        #access to mongodb
        client=pymongo.MongoClient(uri)        
        self.db=client.get_database()
        
    #remove unused key, ObjectId created by mongo can't convert to a json element    
    def remove_keys(self,target,poplist):
        data=[]
        for page in target:
            for key in poplist:
                page.pop(key) #pop keys from the page
            data+=[page]
        return data  
    
    #check whether indicator is valid    
    def check_validation(self,indicator):
        #access to indicator page to find key 'Invalid value'
        url='http://api.worldbank.org/v2/indicator/'+indicator
        req=urlopen(url)
        content=str(req.read())
        if 'Invalid value' in content:
            error=400
        else:
            error=0
        return error

    #get data from world bank
    def get_world_bank_data(self,indicator,page):
        #get indicator content
        #world bank url
        url='http://api.worldbank.org/v2/'
        post_data='countries/%s/indicators/%s?date=%s&format=%s&page=%s'%('all',indicator,'2012:2017','json',page)
        req=urlopen(url+post_data)
        content=req.read()
        
        #retrieve expected value and store in a dict
        data=json.loads(content)[1]
        indicator_value=data[0]['indicator']['value']
        df=pd.DataFrame()
        df['country']=[data[a]['country']['value'] for a in range(len(data))]
        df['date']=[data[a]['date'] for a in range(len(data))]
        df['value']=[data[a]['value'] for a in range(len(data))]
        df_dict=df.to_dict(orient='records')
        return indicator_value,df_dict

    #import data into db
    def import_database(self,collection,max_page):
        indicator=collection
        collection = self.db[collection]
        #for loop to get data
        for page in range(1,max_page+1):
            indicator_value,data=self.get_world_bank_data(indicator,page)  
            creation_time=time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))        
            indicator_dict={'location':'/'+indicator+'/'+str(page),
                            'collection_id':str(page),
                            'indicator':indicator,
                            'indicator_value':indicator_value,
                            'creation_time':creation_time,
                            'entries':data} #store data in a dict
            collection.insert_one(indicator_dict) #insert to database
        data=self.remove_keys(collection.find(),['_id','indicator_value','entries']) #remove _id and irrelevant information
        return data

    #get collection
    def get_collection(self,collection):
        return self.remove_keys(self.db[collection].find(),['_id'])

    
    #get collection id
    def get_collection_id(self,collection,collection_id):
        data=self.remove_keys(self.db[collection].find({'collection_id':collection_id}),['_id'])
        return data
    
    #delete collection id
    def delete_collection_id(self,collection,collection_id):
        self.db[collection].remove({'collection_id':collection_id})
    
    #get collection, collection_id, date ,country
    def get_date_country(self,collection,collection_id,date,country):
        line=IF.db[collection].find_one({'collection_id':collection_id})
        data=pd.DataFrame(line['entries'])
        data=data[data.date==date] #get required date
        data=data[data.country==country] #get required country
        #if data length>0 return json, else return data
        if len(data)>0:
            data_json={'collection_id':collection_id,
                       'indicator':line['indicator'],
                       'country':country,
                       'year':date,
                       'value':data['value'].iloc[0]}
            return data_json
        else:
            return data
        
    #get collection, collection_id, date ,country
    def get_date(self,collection,collection_id,date,query):
        line=IF.db[collection].find_one({'collection_id':collection_id}) #get target information
        line.pop('_id') #remove _id
        data=pd.DataFrame(line['entries']) #convert to dataframe
        data=data[data.date==date] #get required data
        data=data.sort_values('value',ascending=False) #sort
        line.pop('entries') #remove old entries
        #if query exists, get the slice according to the query
        if query!='': 
            query=query.lower() #change to lowercase
            if 'top' in query:
                data=data.iloc[:int(query[len('top'):])]
            elif 'bottom' in query:
                data=data.iloc[-int(query[len('bottom'):]):]
                
        line['entires']=data.to_dict(orient='records') #input new entries
        if len(data)>0:
            return line
        else:
            return data
#    def test(self,collection,collection_id,date,query):
#        b=IF.db[collection].find({'collection_id':collection_id})
#        
#        a=IF.db[collection].find({'entries.date':date},{'entries.$.date':1})
#        for i in a:
#            print(i)
#        c=IF.db[collection].aggregate([{"$unwind":"$entries"}])
#        d=IF.db[collection].aggregate([{"$unwind":"$entries"},
#               {"$match":{"entries.date":date}}])
#        
#        e=IF.db[collection].aggregate([{"$unwind":"$entries"},
#               {"$match":{"entries.date":date}},
#               {"$project":{"entries":1}}])
#        print('hello')
           
#/<string:collection> include get and post
@api.route('/<string:collection>',methods=['GET','POST'],endpoint='collection_id')
class Indicator_API_Collection(Resource):
    
    @api.response(200,'OK')
    @api.response(400,'Inexistence')
    def get(self,collection):
        
        data=IF.get_collection(collection)
        if len(data)>0:
            return data,200
        else:
            return {'message':'Collection={} do not exist in the database'.format(collection)},400
    
    @api.response(200,'OK')
    @api.response(201,'Created')
    @api.response(400,'Invalid')
    def post(self,collection):
        
        if len(IF.get_collection(collection))>0: 
            return {'message':'Collection={} exist in database'.format(collection)},200
        elif IF.check_validation(collection): 
            return {'message':'Invalid indicator'},400
        else:
            data=IF.import_database(collection,2) #max two pages
        return data,201
  
#/<string:collection>/<string:collection_id> include get and delete
@api.route('/<string:collection>/<string:collection_id>',methods=['GET','DELETE'])
class Indicator_API_Collection_ID(Resource):
    @api.response(200,'OK')
    @api.response(400,'Invalid')
    def get(self,collection,collection_id):
        data=IF.get_collection_id(collection,collection_id)
        if len(data)>0:
            return data
        else:
            return {'message':'Collection={} do not exist in the database'.format(collection+'/'+collection_id)},400
        
    @api.response(200,'OK')
    @api.response(400,'Invalid')
    def delete(self,collection,collection_id):
        if len(IF.get_collection_id(collection,collection_id))>0:
            IF.delete_collection_id(collection,collection_id)
            return {'message':'Collection={} is removed from the database!'.format(collection+'/'+collection_id)},200  
        else:
            return {'message':'Collection={} do not exist in the database'.format(collection+'/'+collection_id)},400
            
#/<string:collection>/<string:collection_id>/<string:date>/<string:country> include get
@api.route('/<string:collection>/<string:collection_id>/<string:date>/<string:country>',methods=['GET'])
class Indicator_API_Country(Resource):
    @api.response(200,'OK')
    @api.response(400,'Inexistence')
    def get(self,collection,collection_id,date,country):
        if len(IF.get_collection_id(collection,collection_id))>0:
            data=IF.get_date_country(collection,collection_id,date,country)
            if len(data)>0:
                return data,200
        return {'message':'Collection={} do not exist in the database'.format(collection+'/'+collection_id+'/'+date+'/'+country)},400

#/<string:collection>/<string:collection_id>/<string:date> include get
@api.route('/<string:collection>/<string:collection_id>/<string:date>',methods=['GET'])
@api.param('query')
class Indicator_API_Date(Resource):
    @api.response(200,'OK')
    @api.response(400,'Invalid entry')
    def get(self,collection,collection_id,date):
        query=request.args.get('query', '')
        if len(IF.get_collection_id(collection,collection_id))>0:        
            data=IF.get_date(collection,collection_id,date,query) 
            if len(data)>0:
                return data,200
        return {'message':'Collection={} do not exist in the database'.format(collection+'/'+collection_id+'/'+date)},400

if __name__ == '__main__':
    uri = 'mongodb://admin:admin123@ds261302.mlab.com:61302/wbei' #this is a temporary db
    IF=Indicator_Function(uri)

    # run the application
    app.run(debug=True)        
