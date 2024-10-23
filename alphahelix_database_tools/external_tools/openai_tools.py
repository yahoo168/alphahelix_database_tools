from openai import OpenAI # type: ignore
import tiktoken
import base64
import json

# 待改：multi-thread
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

def cal_GPT_API_expense(text, model="gpt-4o"):
    # 計算token數，"cl100k_base"是與GPT-3和GPT-4模型兼容的分詞器。
    enc = tiktoken.get_encoding("cl100k_base")
    token_num = len(enc.encode(text))
    # GPT-4o US$0.0025 / 1K tokens
    if model == "gpt-4o":
        return token_num * 0.0025 / 1000
    # GPT-3.5-turbo $0.0005 / 1K tokens
    elif model == "gpt-3.5-turbo":
        return token_num * 0.0005 / 1000