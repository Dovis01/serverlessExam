import {APIGatewayProxyHandlerV2} from "aws-lambda";
import {DynamoDBClient} from "@aws-sdk/client-dynamodb";
import {DynamoDBDocumentClient, QueryCommand} from "@aws-sdk/lib-dynamodb";
import Ajv from "ajv";
import schema from "../shared/types.schema.json";

const ajv = new Ajv({coerceTypes: true});
const isValidQueryParams = ajv.compile(schema.definitions["MovieCrewQueryParams"] || {});

const dynamoDbDocClient = createDynamoDbDocClient();

export const handler: APIGatewayProxyHandlerV2 = async (event, context) => {     // Note change
    try {
        console.log("Event: ", event);
        const parameters = event?.pathParameters;
        const movieId = parameters?.movieId ? parseInt(parameters.movieId) : undefined;
        const role = parameters?.role;

        if (!movieId) {
            return {
                statusCode: 404,
                headers: {
                    "content-type": "application/json",
                },
                body: JSON.stringify({Message: "Missing path variable for movie Id."})
            };
        }

        if (!role) {
            return {
                statusCode: 404,
                headers: {
                    "content-type": "application/json",
                },
                body: JSON.stringify({Message: "Missing path variable for the role of crew member."})
            };
        }

        const queryCommandOutput = await dynamoDbDocClient.send(
            new QueryCommand({
                TableName: process.env.TABLE_NAME,
                KeyConditionExpression: "movieId = :movieId AND crewRole = :roleName",
                ExpressionAttributeValues: {
                    ":movieId": movieId,
                    ":roleName": role,
                },
            })
        );

        console.log("QueryCommand response: ", queryCommandOutput);

        if (queryCommandOutput.Items?.length == 0) {
            return {
                statusCode: 404,
                headers: {
                    "content-type": "application/json",
                },
                body: JSON.stringify({Message: "Invalid or wrong movie Id or role name."}),
            };
        }

        const queryParams = event.queryStringParameters;

        if (isValidQueryParams(queryParams)) {
            const queryCommandOutput = await dynamoDbDocClient.send(
                new QueryCommand({
                    TableName: process.env.TABLE_NAME,
                    IndexName: "NamesIx",
                    KeyConditionExpression: "movieId = :m AND contains(names, :n)",
                    ExpressionAttributeValues: {
                        ":m": movieId,
                        ":n": queryParams,
                    },
                })
            );

            return {
                statusCode: 200,
                headers: {
                    "content-type": "application/json",
                },
                body: JSON.stringify({
                    message: "Get crew members by movie id and role which contains substring successfully.",
                    data: queryCommandOutput.Items
                }),
            };
        }

        // Return Response
        return {
            statusCode: 200,
            headers: {
                "content-type": "application/json",
            },
            body: JSON.stringify({
                message: "Get crew members by movie id and role successfully.",
                data: queryCommandOutput.Items
            }),
        };

    } catch (error: any) {
        console.log(JSON.stringify(error));
        return {
            statusCode: 500,
            headers: {
                "content-type": "application/json",
            },
            body: JSON.stringify({error}),
        };
    }
};

function createDynamoDbDocClient() {
    const ddbClient = new DynamoDBClient({region: process.env.REGION});
    const marshallOptions = {
        convertEmptyValues: true,
        removeUndefinedValues: true,
        convertClassInstanceToMap: true,
    };
    const unmarshallOptions = {
        wrapNumbers: false,
    };
    const translateConfig = {marshallOptions, unmarshallOptions};
    return DynamoDBDocumentClient.from(ddbClient, translateConfig);
}
