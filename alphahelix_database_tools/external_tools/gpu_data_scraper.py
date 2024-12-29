import requests
import pandas as pd
from bs4 import BeautifulSoup
import re

def fetch_gpu_pricing(source):
    if source == "coreweave":
        return _fetch_gpu_pricing_from_coreweave()
    elif source == "cudocompute":
        return _fetch_gpu_pricing_from_cudocompute()
    elif source == "runpod":
        return _fetch_gpu_pricing_from_runpod()
    elif source == "datacrunch":
        return _fetch_gpu_pricing_from_datacrunch()
    else:
        print("Invalid source specified. Supported sources: 'cudocompute', 'runpod'.")
        return None

from bs4 import BeautifulSoup
import requests
import re

def _fetch_gpu_pricing_from_coreweave():
    """
    Fetch GPU pricing from CoreWeave.
    """
    url = "https://www.coreweave.com/gpu-cloud-pricing"

    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()  # Raise an error for HTTP issues
    except requests.exceptions.RequestException as error:
        print(f"Error fetching the URL: {error}")
        return None

    soup = BeautifulSoup(response.text, "html.parser")
    table = soup.find("div", class_="table")
    if not table:
        print("Unable to locate the pricing table on the page.")
        return None

    gpu_data_list = []
    rows = table.find_all("div", class_="table-body-row")

    for row in rows:
        try:
            gpu_model_raw = row.find("div", class_="table-body-left").get_text(strip=True)
            gpu_model_clean = re.sub(r"[\u00A0Â]+", " ", gpu_model_raw).strip()  # 去除亂碼
            
            vram = row.find_all("div", class_="w-col w-col-2")[0].get_text(strip=True)
            full_model = f"{gpu_model_clean} {vram}GB"  # 合併 VRAM 至 GPU 型號

            # max_vcpus = row.find_all("div", class_="w-col w-col-2")[1].get_text(strip=True)
            # max_ram = row.find_all("div", class_="w-col w-col-2")[2].get_text(strip=True)
            cost_per_hour_raw = row.find_all("div", class_="w-col w-col-2")[3].get_text(strip=True)
            cost_match = re.search(r"\d+\.\d+", cost_per_hour_raw)
            cost_per_hour = float(cost_match.group(0)) if cost_match else None

            gpu_data_list.append({
                'model': full_model,
                'cost': cost_per_hour,
                "unit": "hr",
            })
        except (AttributeError, ValueError, IndexError) as e:
            print(f"Error parsing row: {e}")
            continue

    return gpu_data_list

# Example usage
if __name__ == "__main__":
    gpu_pricing = _fetch_gpu_pricing_from_coreweave()
    if gpu_pricing:
        for gpu in gpu_pricing:
            print(gpu)


def _fetch_gpu_pricing_from_cudocompute():
    """
    Fetch GPU pricing from CudoCompute.
    """
    url = "https://www.cudocompute.com/pricing"
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36"
    }

    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
    except requests.exceptions.RequestException as error:
        print(f"Error fetching the URL: {error}")
        return None

    soup = BeautifulSoup(response.text, "html.parser")
    table = soup.select_one("table.min-w-full")
    if not table:
        print("Unable to locate the pricing table on the page.")
        return None

    gpt_data_list = []
    for row in table.find("tbody").find_all("tr"):
        cells = row.find_all("td")
        if len(cells) >= 2:
            model_element = cells[0].find("span", class_="font-medium")
            memory_element = cells[0].find("span", class_="text-xs")
            model = model_element.get_text(strip=True) if model_element else ""
            memory = memory_element.get_text(strip=True) if memory_element else ""
            full_model_raw_text = f"{model} {memory}"
            full_model = re.sub(r"\s*\(.*?\)", "", full_model_raw_text)

            cost_element = cells[1].get_text(strip=True)
            cost_match = re.search(r"\d+\.\d+", cost_element)
            cost = cost_match.group(0) if cost_match else None

            gpt_data_list.append({
                'model': full_model,
                'cost': float(cost) if cost else None,
                'unit': "hr"
            })

    return gpt_data_list

def _fetch_gpu_pricing_from_runpod():
    """
    Fetch GPU pricing from Runpod.io.
    """
    url = "https://www.runpod.io/pricing"
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36"
    }

    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
    except requests.exceptions.RequestException as error:
        print(f"Error fetching the URL: {error}")
        return None

    soup = BeautifulSoup(response.text, "html.parser")
    gpu_cards = soup.select(".MuiGrid-root.MuiGrid-item.MuiGrid-grid-xs-12")
    if not gpu_cards:
        print("Unable to locate GPU cards on the page.")
        return None

    gpt_data_list = []
    for card in gpu_cards:
        model_element = card.select_one(".MuiTypography-body1.css-6ukrhs")
        memory_element = card.select_one(".MuiTypography-body1.css-1xqiyyp")
        model = model_element.text.strip() if model_element else None
        memory = memory_element.text.strip() if memory_element else None

        full_model = f"{model} {memory}" if model and memory else model

        price_elements = card.select(".MuiTypography-body1.css-c16693")
        secure_cloud_price = re.sub(r"[^\d.]", "", price_elements[0].text.strip()) if len(price_elements) > 0 else None
        community_cloud_price = re.sub(r"[^\d.]", "", price_elements[1].text.strip()) if len(price_elements) > 1 else None

        gpt_data_list.append({
            'model': full_model,
            'cost': float(community_cloud_price) if community_cloud_price else None,
            'cost_secure_cloud': float(secure_cloud_price) if secure_cloud_price else None,
            'unit': "hr"
        })

    return gpt_data_list

def _fetch_gpu_pricing_from_datacrunch():
    url = "https://datacrunch.io/products"
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36"
    }
    try:    
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Raise HTTPError for bad responses
    except requests.RequestException as e:
        print(f"Error fetching URL: {e}")
        return None

    # Parse the HTML
    soup = BeautifulSoup(response.text, 'html.parser')
    data = []

    # Locate all GPU tables
    tables = soup.find_all('table', class_='fvBnbK')

    for table in tables:
        # Extract headers
        headers = [header.get_text(strip=True) for header in table.find_all('th')]

        # Extract rows
        tbody = table.find('tbody')
        if tbody:
            rows = tbody.find_all('tr')
            for row in rows:
                values = [value.get_text(strip=True) for value in row.find_all('td')]
                if len(values) == len(headers):
                    data.append(dict(zip(headers, values)))

    # Convert to DataFrame
    if not data:
        print("No data found in the tables.")
        return None
    df = pd.DataFrame(data)

    # Drop rows where "GPU model" is NaN
    df.dropna(subset=["GPU model"], inplace=True)

    # Remove the '$' and '/h' from "On demand price" and convert to float
    df["On demand price"] = df["On demand price"].apply(lambda x: float(re.sub(r'\$|/h', '', x)))

    # Create a new column for the full model description
    df["full_model"] = df["GPU model"].astype(str) + " GPU-" + df["GPU"].fillna("0").astype(str) + " CPU-" + df["CPU"].fillna("0").astype(str)

    # Create the list of dictionaries
    gpt_data_list = [
        {
            'model': row["full_model"],
            'cost': float(row["On demand price"]) if row["On demand price"] else None,
            'unit': "hr"
        }
        for _, row in df.iterrows()
    ]

    return gpt_data_list