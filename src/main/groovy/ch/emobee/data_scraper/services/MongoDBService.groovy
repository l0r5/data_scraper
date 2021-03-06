package ch.emobee.data_scraper.services

import ch.emobee.data_scraper.utils.DataFormatUtils
import com.mongodb.DBCursor
import com.mongodb.client.FindIterable
import com.mongodb.client.MongoClient
import com.mongodb.client.MongoClients
import com.mongodb.client.MongoCollection
import com.mongodb.client.MongoDatabase
import org.bson.Document

class MongoDBService {

    def static save(Object obj, databaseName, collectionName) {
        MongoClient mongoClient = MongoClients.create()
        MongoDatabase db = mongoClient.getDatabase(databaseName)
        MongoCollection<Document> collection = db.getCollection(collectionName)
        Document doc = DataFormatUtils.parseToMongoDoc(obj)
        collection.insertOne(doc)
    }

    def static findAll(){
        List <Document> allDocs = []
        MongoClient mongoClient = MongoClients.create()
        MongoDatabase db = mongoClient.getDatabase("train-delays")
        MongoCollection<Document> collection = db.getCollection("extract-scheduled-actual-arrival")
        FindIterable<Document> docs = collection.find()
        for(Document doc : docs) {
            allDocs.add(doc)
        }
        return allDocs
    }

}
