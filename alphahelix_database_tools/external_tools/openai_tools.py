from openai import OpenAI # type: ignore
import tiktoken

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

def cal_GPT_API_expense(text, model="gpt-4o"):
    # 計算token數，"cl100k_base"是與GPT-3和GPT-4模型兼容的分詞器。
    enc = tiktoken.get_encoding("cl100k_base")
    token_num = len(enc.encode(text))
    # GPT-4o US$0.005 / 1K tokens
    if model == "gpt-4o":
        return token_num * 0.005 / 1000
    # GPT-3.5-turbo $0.0005 / 1K tokens
    elif model == "gpt-3.5-turbo":
        return token_num * 0.0005 / 1000