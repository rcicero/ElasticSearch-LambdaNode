'use strict';

const AWS = require('aws-sdk');

const SQS = new AWS.SQS({ apiVersion: '2012-11-05' });
const Lambda = new AWS.Lambda({ apiVersion: '2015-03-31' });
const http = require('http');

// Your queue URL stored in the queueUrl environment variable
const QUEUE_URL = process.env.queueUrl;
const PROCESS_MESSAGE = 'process-message';

// Your ES URL path
const ES_URL = process.env.esUrl;


function processMessage(message, callback) {
    
    
    var messageJson = JSON.parse(message.Body);

    console.log("Action Type is: " + messageJson.actionType);
    
    switch(messageJson.actionType) {
    case 'CREATE':
        postElasticSearch(message, messageJson, callback);
        break;
    case 'UPDATE':
        postElasticSearch(message, messageJson, callback);
        break;
    case 'DELETE':
        deleteDocumentElasticSearch(message, messageJson.id, callback);
        break;        
    default: 
        console.log("Action Type Error - Invalid Type: "+messageJson.actionType);
    }

}

function poll(functionName, callback) {
    const params = {
        QueueUrl: QUEUE_URL,
        MaxNumberOfMessages: 3,
        VisibilityTimeout: 10,
    };
    
    // batch request messages
    SQS.receiveMessage(params, (err, data) => {
        if (err) {
            return callback(err);
        } else {
            
            if(data.Messages !== undefined){
                data.Messages.forEach(function(message) {
                    processMessage(message, callback);
                });     
            }
                   
        }
        
    });
}

function postElasticSearch(message, messageJson, callback){
	

	//Elastic Search Document Fields    
	var fields = {
            id: messageJson.id, 
            name: messageJson.name, 
            description: messageJson.description 
        };
        
        var options = {
            host: ES_URL,
            path: '/{ElasticSearchIndice}/{ElasticSearchMapping}/'+messageJson.id,
            method: 'POST'
        };
        
     
        var req = http.request(options, function(res) {
            res.setEncoding('utf8');
            res.on('data', function (chunk) {
                
                // delete message
                const params = {
                    QueueUrl: QUEUE_URL,
                    ReceiptHandle: message.ReceiptHandle,
                };
                SQS.deleteMessage(params, (err) => callback(err, message));
                
            });
            res.on('end', function (error) {
                console.log(error);
            });
        });
        
        req.write(JSON.stringify(fields));
        req.end();
        
}

function deleteDocumentElasticSearch(message, id, callback){
        
        var options = {
            host: ES_URL,
            path: '/{ElasticSearchIndice}/{ElasticSearchMapping}/'+id,
            method: 'DELETE'
        };
        
        var req = http.request(options, function(res) {
            res.setEncoding('utf8');
            res.on('data', function (chunk) {
                
                
                // delete message
                const params = {
                    QueueUrl: QUEUE_URL,
                    ReceiptHandle: message.ReceiptHandle,
                };
                SQS.deleteMessage(params, (err) => callback(err, message));
                
            });
            res.on('end', function (error) {
                console.log(error);
            });
        });
        
        req.end();
        
}

exports.handler = (event, context, callback) => {
    try {
       poll(context.functionName, callback);
    } catch (err) {
        callback(err);
    }
};
