import requests
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import pandas as pd
import re
import random
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed

# 1. Kết nối đến MongoDB
uri = "mongodb+srv://hoag11:111204@cluster0.8ef9n.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
client = MongoClient(uri, server_api=ServerApi('1'))
db = client['TikiProductions']
collection = db['Productions']


# 2. Hàm chuẩn hóa mô tả sản phẩm
def normalize_description(description):
    soup = BeautifulSoup(description, "html.parser")
    description = soup.get_text()
    description = description.strip()
    description = description.lower()
    description = re.sub(r'[^\w\s]', '', description)
    return description


# 3. Đọc danh sách ID sản phẩm từ file CSV
csv_file_path = "products-0-200000(in).csv"
product_ids_df = pd.read_csv(csv_file_path)


# 4. Gửi yêu cầu API và lưu dữ liệu vào MongoDB
def fetch_and_save_product_data(product_id):
    # Kiểm tra nếu sản phẩm với ID này đã tồn tại trong MongoDB
    if collection.find_one({"_id": str(product_id)}):
        print(f"Sản phẩm {product_id} đã tồn tại trong MongoDB. Bỏ qua.")
        return

    url = f'https://api.tiki.vn/product-detail/api/v1/products/{product_id}'
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36'
    }

    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            data = response.json()

            # Chuẩn hóa phần mô tả
            if 'description' in data:
                data['description'] = normalize_description(data['description'])

            # Lưu dữ liệu vào MongoDB với ID sản phẩm là _id để tránh trùng lặp
            data['_id'] = str(product_id)  # Sử dụng ID sản phẩm làm _id
            collection.insert_one(data)
            print(f"Sản phẩm {product_id} đã được lưu vào MongoDB.")
        else:
            print(f"Lỗi khi lấy dữ liệu sản phẩm {product_id}. Mã trạng thái: {response.status_code}")
    except Exception as e:
        print(f"Lỗi khi lấy dữ liệu sản phẩm {product_id}: {e}")


# 5. Chạy đa luồng để xử lý nhiều yêu cầu đồng thời
def main():
    num_threads = 5  # Sử dụng 5 luồng để tăng tốc độ xử lý
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = [executor.submit(fetch_and_save_product_data, product_id) for product_id in product_ids_df['id']]

        for future in as_completed(futures):
            try:
                future.result()
            except Exception as exc:
                print(f"Lỗi xảy ra: {exc}")


if __name__ == "__main__":
    main()
