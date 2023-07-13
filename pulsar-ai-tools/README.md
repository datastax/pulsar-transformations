## Streaming Generative AI Extensions

This module is an extensions of the standard Transformation function,
and add some useful functions for Streaming generative AI.

Currently available GenAI operations are:
* [compute-ai-embeddings](#compute-ai-embeddings): computes an Embeddings vector for a given text.
* [query](#qyery): queries a database and enrich the message with the results.
* [ai-chat-completions](#ai-chat-completions): calls an AI model to generate a text completion.


### Compute AI Embeddings

Step name: `compute-ai-embeddings`

Computes an embeddings vector for a given text. The embeddings vector is added to the message.

Parameters:

| Name             | Description                                                                                                         |
|------------------|---------------------------------------------------------------------------------------------------------------------|
| model            | The model to use, it depends on the provider                                                                        |
| compute-service  | The embeddings provider (openai, huggingface...)                                                                    | 
| text             | Template for the text to compute the embeddings on. You can use the Mustache syntax (for example {{value.field1}} ) |
| embeddings-field | The name of the field to add or update (for example value.embeddingsvector)                                         |


### Query

Step name: `query`

Executes a query against the database configured in the "datasource" section.

Parameters:

| Name         | Description                                                                 |
|--------------|-----------------------------------------------------------------------------|
| output-field | The name of the field to add or update (for example value.embeddingsvector) |
| query        | Template for the query, you can use '?' for parameters                      |
| fields       | An array of field names to use to fill-in the parameters of the query       |


### AI Chat Completion

Step name: `ai-chat-completions`

Calls a Completion AI model to generate a text completion.

Parameters:

| Name              | Description                                                                                                                                                                                                                                 |
|-------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| output-field      | The name of the field to add or update (for example value.embeddingsvector)                                                                                                                                                                 |
| messages          | Template for the messages, it is an array. You can use Mustache in order to fill in the template with the fields of the message.                                                                                                            |
| model             | ID of the model to use. See the [model endpoint compatibility](https://platform.openai.com/docs/models/model-endpoint-compatibility) table for details on which models work with the Chat API.                                              |
| temperature       | What sampling temperature to use, between 0 and 2. Higher values like 0.8 will make the output more random, while lower values like 0.2 will make it more focused and deterministic.                                                        |
| top-p             | An alternative to sampling with temperature, called nucleus sampling, where the model considers the results of the tokens with top_p probability mass. So 0.1 means only the tokens comprising the top 10% probability mass are considered. |
| stop              | Up to 4 sequences where the API will stop generating further tokens.                                                                                                                                                                        |
| max-tokens        | The maximum number of [tokens](https://platform.openai.com/tokenizer) to generate in the chat completion.                                                                                                                                   |
| presence-penalty  | Number between -2.0 and 2.0. Positive values penalize new tokens based on whether they appear in the text so far, increasing the model's likelihood to talk about new topics.                                                               |
| frequency-penalty | Number between -2.0 and 2.0. Positive values penalize new tokens based on their existing frequency in the text so far, decreasing the model's likelihood to repeat the same line verbatim.                                                  |
| user              | A unique identifier representing your end-user, which can help OpenAI to monitor and detect abuse. [Learn more](https://platform.openai.com/docs/guides/safety-best-practices/end-user-ids).                                                |

