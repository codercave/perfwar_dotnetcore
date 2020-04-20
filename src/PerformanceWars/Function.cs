using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Net.Http;
using Newtonsoft.Json;

using Amazon.Lambda.Core;
using Amazon.Lambda.APIGatewayEvents;

using System.Security.Cryptography;
using System.Text;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DocumentModel;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.Json.JsonSerializer))]

namespace PerformanceWars
{

    public class Function
    {
        private static AmazonDynamoDBClient client;
        private static string tableRef;
        private static Table table;

        static Function(){
            client = new AmazonDynamoDBClient();
            tableRef = Environment.GetEnvironmentVariable("Table");
            table = Table.LoadTable(client, tableRef);
        }

        private string hash(string document){
            string digest = "";
            SHA256 sha = SHA256.Create();

            var docBytes = Encoding.ASCII.GetBytes(document);
            var hash = sha.ComputeHash(docBytes);

            digest = BitConverter.ToString(hash).Replace("-", "");

            return digest;
        }

        public async Task<APIGatewayProxyResponse> FunctionHandler(APIGatewayProxyRequest apigProxyEvent, ILambdaContext context)
        {
            Console.WriteLine("table name: " + Function.tableRef);

            if(apigProxyEvent.HttpMethod == "POST") 
                return await PostHandler(apigProxyEvent, context);
            else
                return await GetHandler(apigProxyEvent, context);
        }
        
        private async Task<APIGatewayProxyResponse> PostHandler(APIGatewayProxyRequest apigProxyEvent, ILambdaContext context)
        {
            var payload = JsonConvert.DeserializeObject<Payload>(apigProxyEvent.Body);
            string docHash = hash(payload.Document);
            payload.Hash = docHash;
            var document = Payload.ToDynamodbDocument(payload);

            Expression expr = new Expression();
            expr.ExpressionStatement = "ID <> :id";
            expr.ExpressionAttributeValues[":id"] = payload.ID;
            PutItemOperationConfig putConfig = new PutItemOperationConfig{
                ConditionalExpression = expr,
                ReturnValues = ReturnValues.AllOldAttributes
            };
            
            try{
                table.PutItemAsync(document, putConfig).Wait();
            }catch(Exception e){
                return new APIGatewayProxyResponse
                {
                    Body = JsonConvert.SerializeObject(new Dictionary<string, string>{{"id", payload.ID.ToString()}, {"message", "object already exists"}}),
                    StatusCode = 409,
                    Headers = new Dictionary<string, string> { { "Content-Type", "application/json" } }
                };
            }
            

            var body = new Dictionary<string, string>
            {
                {"id", payload.ID.ToString()},
                { "hash", docHash }
            };

            return new APIGatewayProxyResponse
            {
                Body = JsonConvert.SerializeObject(body),
                StatusCode = 201,
                Headers = new Dictionary<string, string> { { "Content-Type", "application/json" } }
            };
        }

        private async Task<APIGatewayProxyResponse> GetHandler(APIGatewayProxyRequest apigProxyEvent, ILambdaContext context)
        {
            if(apigProxyEvent.QueryStringParameters.ContainsKey("id"))
            {
                var item = await table.GetItemAsync(apigProxyEvent.QueryStringParameters["id"]);

                if(item == null)
                {
                    return new APIGatewayProxyResponse
                    {
                        Body = JsonConvert.SerializeObject(item),
                        StatusCode = 404,
                        Headers = new Dictionary<string, string> { { "Content-Type", "application/json" } }
                    };
                }

                return new APIGatewayProxyResponse
                {
                    Body = JsonConvert.SerializeObject(item),
                    StatusCode = 200,
                    Headers = new Dictionary<string, string> { { "Content-Type", "application/json" } }
                };
            }

            return new APIGatewayProxyResponse
            {
                Body = JsonConvert.SerializeObject(new Dictionary<string, string> {{"error", "something went wrong"}}),
                StatusCode = 500,
                Headers = new Dictionary<string, string> { { "Content-Type", "application/json" } }
            };
        }

    }

    public class Payload
    {
        public Guid ID { get; set; }
        public string Document { get; set; }
        public string Hash { get; set; }

        public static Document ToDynamodbDocument(Payload payload)
        {
            Document document = new Document();
            document["ID"] = payload.ID;
            document["Document"] = payload.Document;
            document["Hash"] = payload.Hash;

            return document;
        }
    }
}
