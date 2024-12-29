from openai import OpenAI # type: ignore
import tiktoken, base64

from alphahelix_database_tools.external_tools.pdf_tools import clean_gibberish_text, truncate_text_to_token_limit
import numpy as np
import os

from dotenv import load_dotenv #type: ignore
# 應該放在這？
load_dotenv()

def call_OpenAI_API(API_key, prompt, model_version="gpt-4o", output_format="text"):
    client = OpenAI(api_key= API_key)
    chat_completion = client.chat.completions.create(
        messages = [
            {
                "role": "user",
                "content": prompt,
            }
        ],
        model = model_version,
        response_format={"type": output_format}
    )
    return chat_completion.choices[0].message.content
    
def call_OpenAI_for_image(API_key, text_prompt, image_path_list, is_local_file=False, model="gpt-4o-mini", max_tokens=None, output_format="json_object"):
    def encode_image(image_path):
        """Encode a local image file to base64."""
        with open(image_path, "rb") as image_file:
            return base64.b64encode(image_file.read()).decode('utf-8')
    
    client = OpenAI(api_key=API_key)
    messages = [
        {
            "role": "user",
            "content": [
                {"type": "text", "text": text_prompt},
            ] + [
                {
                    "type": "image_url",
                    "image_url": {
                        "url": f"data:image/jpeg;base64,{encode_image(image_path)}" if is_local_file else image_path
                    },
                }
                for image_path in image_path_list
            ]
        }
    ]

    response = client.chat.completions.create(
        model=model,
        messages=messages,
        max_tokens=max_tokens,
        response_format={"type": output_format},
    )
    return response.choices[0].message.content

def cal_GPT_API_expense(text, model):
    # 計算token數，"cl100k_base"是與GPT-3和GPT-4模型兼容的分詞器。
    enc = tiktoken.get_encoding("cl100k_base")
    token_num = len(enc.encode(text))
    
    # GPT-4o US$0.0025 / 1K tokens
    if model == "gpt-4o":
        return token_num * 6 / 10**6 #取input & output 價格的平均
    
    elif model == "text-embedding-3-small":
        return token_num * 0.02 / 10**6
    
    elif model == "text-embedding-3-large":
        return token_num * 0.13 / 10**6
    
    else:
        raise ValueError("Model not supported.")


def cosine_similarity(vec1, vec2):
    return np.dot(vec1, vec2) / (np.linalg.norm(vec1) * np.linalg.norm(vec2))

def get_embedding(text, clean_text=True, quantize=True):
    def _get_embedding_LLM(text, model="text-embedding-3-large"):
        client = OpenAI(api_key= os.getenv('OpenAI_API_key')) #待改：重新整理
        text = text.replace("\n", " ")
        return client.embeddings.create(input = [text], model=model).data[0].embedding

    if clean_text:
        text = clean_gibberish_text(text)
    
    text = truncate_text_to_token_limit(text, token_limit=8000)
    embedding = _get_embedding_LLM(text)
    
    if quantize:
        embedding_in8, _ = _quantize_to_int8(embedding)
        return embedding_in8.tolist()
    else:
        return embedding

# 將float32轉為int8（縮小嵌入向量的大小）
def _quantize_to_int8(embedding):
    """
    将嵌入向量从 float32 转为 int8 格式，同时返回量化比例（scale）。
    """
    min_val, max_val = np.min(embedding), np.max(embedding)
    scale = max(abs(min_val), abs(max_val))
    quantized = np.round((embedding / scale) * 127).astype(np.int8)
    return quantized, scale

# 提供反量化方法：将 int8 转为 float32
def _dequantize_from_int8(quantized_embedding, scale):
    """
    将嵌入向量从 int8 格式还原到 float32 格式。
    """
    return (quantized_embedding.astype(np.float32) / 127) * scale

def vector_search(query_text, collection, similarity_threshold=0.6):
    # Generate embedding for the user query
    query_embedding = get_embedding(query_text, clean_text=False)

    if query_embedding is None:
        return "Invalid query or embedding generation failed."

    # Define the vector search pipeline
    pipeline = [
        {
            "$vectorSearch": {
                "index": "embedding_int8",
                "queryVector": query_embedding,
                "path": "embedding",
                "numCandidates": 100,  # Number of candidate matches to consider
                "limit": 10  # Return top 10 matches
            }
        },
        {
             "$addFields": {"score": {"$meta": "vectorSearchScore"}}  # add the similarity score in fields
        },
        {
            "$match": {
                "score": {"$gte": similarity_threshold}  # Filter by cosine similarity threshold
            }
        },
        {
            "$project": {
                "_id": 0,  # Exclude the _id field
                "title": 1,
                "url": 1,
                "score": 1,  # Include the similarity score in results
            }
        }
    ]

    # Execute the search
    results = collection.aggregate(pipeline)
    return list(results)