# -*- coding: utf-8 -*-
"""
Created on Wed Sep 19 10:00:40 2018

@author: Charmaine
"""
'''
This is a world bank economic indicator data service.
There are mainly two parts, indicator_function and indicator api. 
'''
import pymongo,json,time
from urllib.request import urlopen
import pandas as pd
from bson import ObjectId
from flask import Flask,request
from flask_restplus import fields,Resource, Api



#This part is the interaction from the api to database
class Indicator_Function():
    def __init__(self,username,password,databases):
        #access to mongodb
        uri = 'mongodb://%s:%s@%s'%(username,password,databases)
        client=pymongo.MongoClient(uri)        
        db=client.get_database()
        self.collection=db['indicator']

    #exist return 0, not exist return 1
    def check_indicator_existence(self,indicator):
        if type(self.collection.find_one({'indicator':indicator}))==dict:
            return 200
        else:
            return 400
    
    #check whether indicator is valid    
    def check_validation(self,indicator):
        #access to indicator page to find key 'Invalid value'
        url='http://api.worldbank.org/v2/indicator/'+indicator
        req=urlopen(url)
        content=str(req.read())
        if 'Invalid value' in content:
            return 400
        else:
            return 0

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
        return indicator_value,df

    #import data into db
    def import_database(self,indicator,max_page):
        data_dict={}
        #for loop to get data
        for page in range(1,max_page+1):
            indicator_value,data_dict[page]=self.get_world_bank_data(indicator,page)  
        
        data=pd.concat(data_dict).to_dict(orient='records')
        creation_time=time.strftime('%Y-%m-%dT%H:%M:%SZ', time.localtime(time.time()))        
        indicator_dict={'indicator':indicator,
                        'indicator_value':indicator_value,
                        'creation_time':creation_time,
                        'entries':data} #store data in a dict
        self.collection.insert_one(indicator_dict) #insert to database
    
    def data_format(self,data):
        return {'location':'/indicators/'+str(data['_id']),
                'collection_id':str(data['_id']),
                'creation_time':data['creation_time'],'indicator':data['indicator']}
    
    #get data of the indicator
    def get_indicator(self,indicator):
        data=self.collection.find_one({'indicator':indicator})
        return (self.data_format(data))    
        
    #get data of collection
    def get_collection(self):
        data=[]
        for line in self.collection.find():
            data.append(self.data_format(line))
        return data
    
    #get collection id
    def get_collection_id(self,collection_id):
        try:
            data=self.collection.find_one({'_id':ObjectId(collection_id)})           
                
            return ({'collection_id': str(data['_id']),
                     'indicator': data['indicator'],
                     'indicator_value': data['indicator_value'],
                     'creation_time': data['creation_time'],
                     'entries': data['entries']})
        except:
            return 400
    
    #delete collection id
    def delete_collection_id(self,id):
        try:
            data=self.collection.find_one({'_id':ObjectId(id)})            
            if type(data)==dict:
                self.collection.remove({'_id':ObjectId(id)})
#                data=self.collection.find_one({'_id':ObjectId(collection_id)}) 
                return 200
            return 400
        except:
            return 400
    
    #get collection, collection_id, date ,country
    def get_date_country(self,collection_id,date,country):
        try:
            line=self.collection.find_one({'_id':ObjectId(collection_id)})
            data=pd.DataFrame(line['entries'])
            data=data[data.date==date] #get required date
            data=data[data.country==country] #get required country
            return ({'collection_id':str(line['_id']),
                     'indicator':line['indicator'],
                     'country':country,
                     'year':date,
                     'value':data['value'].iloc[0]})
        except:
            return 400

        
    #get collection, collection_id, date ,country
    def get_date(self,collection_id,date,query):
        try:
            line=self.collection.find_one({'_id':ObjectId(collection_id)}) #get target information
            data=pd.DataFrame(line['entries']) #convert to dataframe
            data=data[data.date==date] #get required data
            data=data.sort_values('value',ascending=False) #sort
            #if query exists, get the slice according to the query
            if query!='': 
                query=query.lower() #change to lowercase
                if 'top' in query:
                    data=data.iloc[:int(query[len('top'):])]
                elif 'bottom' in query:
                    data=data.iloc[-int(query[len('bottom'):]):]
            return ({'indicator':line['indicator'],
                     'indicator_value':line['indicator_value'],
                     'entries':data.to_dict(orient='records')})
        except:
            return 400

#define api
app = Flask(__name__)
api = Api(app,
          default='World Bank Economic Indicator',
          title='World Bank Economic Indicator Data Service',
          description='This is a data service for world bank economic indicator')
model = api.model('Indicator', {'indicator_id': fields.String})
                    
#/indicators include get and post
@api.route('/indicators',methods=['GET','POST'],endpoint='collection_id')
class Indicator_API_Collection(Resource):
    
    @api.response(200,'OK')
    @api.doc(description='Retrieve the list of available collections')
    def get(self):        
        data=IF.get_collection()
        return data,200
    
    @api.expect(model)
    @api.response(200,'OK')
    @api.response(201,'Created')
    @api.response(400,'Error')
    @api.doc(description='Import a collection from the data service')
    def post(self):        
        
        indicator=request.json['indicator_id']
        
        if IF.check_indicator_existence(indicator)==200: 
            return {'message':'Collection={} exist in database'.format(indicator)},200
        elif IF.check_validation(indicator)==400: 
            return {'message':'Error'},400
        else:
            IF.import_database(indicator,2) #max two pages
            data=IF.get_indicator(indicator)
        return data,201
  
#/indicators/<string:collection_id> include get and delete
@api.route('/indicators/<string:collection_id>',methods=['GET','DELETE'])
@api.param('collection_id','collection id')
class Indicator_API_Collection_ID(Resource):
    
    @api.response(200,'OK')
    @api.response(400,'Error')
    @api.doc(description='Retrieve a collection')
    def get(self,collection_id):
        data=IF.get_collection_id(collection_id)
        if data==400:
            return {'message':'Error'},400
        else:
            return data,200
        
    @api.response(200,'OK')
    @api.response(400,'Error')
    @api.doc(description='Deleting a collection with the data service')
    def delete(self,collection_id):
        flag=IF.delete_collection_id(collection_id)
        if flag==400:
            return {'message':'Error'},400
        else:   
            return {'message':'Collection=/indicators/{} is removed from the database!'.format(collection_id)},200  
        
#/indicators/<string:collection_id>/<string:year>/<string:country> include get
@api.route('/indicators/<string:collection_id>/<string:year>/<string:country>',methods=['GET'])
@api.param('collection_id', 'collection id')
@api.param('year', 'given year')
@api.param('country', 'given country')
class Indicator_API_Country(Resource):
    
    @api.response(200,'OK')
    @api.response(400,'Error')
    @api.doc(description='Retrieve economic indicator value for given country and a year')
    def get(self,collection_id,year,country):
        data=IF.get_date_country(collection_id,year,country)
        if data==400:
            return {'message':'Error'},400
        else:
            return data,200


#/indicators/<string:collection_id>/<string:year> include get
@api.route('/indicators/<string:collection_id>/<string:year>',methods=['GET'])
@api.param('collection_id', 'collection id')
@api.param('year', 'given year')
@api.param('query','top<n> or bottom<n>, n is the number of countries')
class Indicator_API_Date(Resource):
    
    @api.response(200,'OK')
    @api.response(400,'Error')
    @api.doc(description='Retrieve top/bottom economic indicator values for a given year')
    def get(self,collection_id,year):
        query=request.args.get('query', '')
        data=IF.get_date(collection_id,year,query)
        if data==400:
            return {'message':'Error'},400
        else:
            return data,200

if __name__ == '__main__':
    username='admin' #database username
    password='admin123' #database password
    database='ds261302.mlab.com:61302/wbei' #database name
    IF=Indicator_Function(username,password,database)
    # run the application
    app.run(debug=True)       
