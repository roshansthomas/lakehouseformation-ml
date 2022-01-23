/*-
 * #%L
 * SentimentAnalysisUDFHandler
 * %%
 * Copyright (C) 2019 - 2022 Amazon Web Services
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

package com.amazonaws.athena.udf.sentimentanalytics;

import com.amazonaws.athena.connector.lambda.handlers.UserDefinedFunctionHandler;

import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.sagemakerruntime.model.InvokeEndpointRequest;
import software.amazon.awssdk.services.sagemakerruntime.model.InvokeEndpointResponse;
import software.amazon.awssdk.services.sagemakerruntime.SageMakerRuntimeClient;
import com.google.gson.Gson;
import com.google.gson.JsonParser;
//import com.google.gson.JsonElement;
//import com.google.gson.JsonArray;
//import org.json.JSONObject;

public class SentimentAnalysisUDFHandler extends UserDefinedFunctionHandler
{
    private static final String SOURCE_TYPE = "athena_sentimentanalytics_udf";
    public SentimentAnalysisUDFHandler()
    {
        super(SOURCE_TYPE);
    }
    private String createRequestString(String input)
    {
        String quot = "\"";
        String returnString = "{\"instances\": [";
        returnString = returnString + quot + input + quot;
    /*  // If String array being passed 
        String comma = ",";
        for (int i = 0; i <= input.length - 1; i++) {
            returnString = returnString + quot;
            returnString = input[i];
            returnString = returnString + quot;
            if (input[i + 1] != null) {
                returnString = returnString + comma;
            }
        }
        */
        returnString = returnString + "]}";
        return returnString;
    }
    private static String toJSON(Object obj) 
    {
        Gson gson = new Gson();
        return gson.toJson(obj);
    }

    private static String[] fromJSON(String json) 
    {
        Gson gson = new Gson();
        return gson.fromJson(json, String[].class);
    }
    /**
        fetchSentiment - Translate the Label returned to the Sentiment
    */

    String fetchSentiment(String labelString)
    {
        String returnVal;
        switch(labelString) {
          case "__label__1":
              returnVal = "Negative";
            // code block
            break;
          case "__label__2":
              returnVal = "Neutral";
            // code block
            break;
          case "__label__3":
              returnVal = "Postive";
            // code block
            break;
            default:
                returnVal = "NotFound";
        }
        return returnVal;
    }
    /**
        extractLabel - Extract the Label from String from the SageMaker invokeendpoint response
    */ 
    public String extractLabel(InvokeEndpointResponse response)
    {
        // Parse the reponse received from the SageMaker Endpoint.
        // the result.body() returns SdkBytes object. This is converted into a String before being parsed
        String responseJson = response.body().asUtf8String();
        // Sample responseJson below
        /*
         [
              {
                "label": [
                  "__label__1"
                ],
                "prob": [
                  0.5706868171691895
                ]
              }
            ]
        */
        
        // 
        String labelString = new JsonParser().parse(responseJson).
                                        getAsJsonArray(). // Refer to the sample response above.Get Array of Label and probability records.
                                        get(0). // Get the first Element of Label and Probability
                                        getAsJsonObject(). // Get the JSON object for Label and Probability Element
                                        get("label"). // Get the Label Element
                                        getAsJsonArray(). //Get the JSON Array from the Element
                                        get(0). // Get the first label record from Array
                                        getAsString(); // Get String value
//        ;
//        JsonArray jsonArray = jsonElement.getAsJsonArray();
//        jsonElement = jsonArray.get(0);
//        JsonObject jsonObject = jsonElement.getAsJsonObject();
//        json = jsonObject.get("label").getAsJsonArray().get(0).getAsString();
        
        return labelString;    
    }
    
    public String detect_bt_sentiment(String input) throws Exception
    {
        //Create the requestString as a JSON string
        String requestString = createRequestString(input);
        // Set the Endpoint Name
        String endpointName = "bt-sentiment-analysis";
        // BlazingText accepts JSON input.. See https://docs.aws.amazon.com/sagemaker/latest/dg/blazingtext.html#bt-inputoutput
        String contentType = "application/json";
        // Invoke SageMaker Endpoint.
        // SageMaker requires the requestbody to be of type SdkBytes.
        // SdkBytes.fromUtf8String(requestString) converts the requestString to SdkBytes
        InvokeEndpointRequest request = InvokeEndpointRequest.builder().contentType(contentType).body(SdkBytes.fromUtf8String(requestString)).endpointName(endpointName).build();
        // A SageMakerRuntimeClient object is required to invoke the SageMakerEndpoint
        SageMakerRuntimeClient runtime = SageMakerRuntimeClient.create();
        // InvokeEndpoint and store the response object of type InvokeEndpointResponse in 'result'
        InvokeEndpointResponse response = runtime.invokeEndpoint(request);
        // Extract label from the response received from invoking the Endpoint
        String labelString = extractLabel(response);    
        System.out.println("String Fetched->" + labelString);
        return fetchSentiment(labelString);
    }
}
