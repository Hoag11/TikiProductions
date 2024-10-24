import requests
from pymongo.mongo_client import MongoClient
import pandas as pd
import re
from bs4 import BeautifulSoup
import logging

# 1. Cấu hình logging để ghi lại log thành công và log lỗi vào file khác nhau
logging.basicConfig(filename='product_log.txt', level=logging.INFO, format='%(asctime)s - %(message)s')

# Logger cho các lỗi
error_logger = logging.getLogger('error_logger')
error_handler = logging.FileHandler('error_log.txt')
error_handler.setLevel(logging.ERROR)
error_format = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
error_handler.setFormatter(error_format)
error_logger.addHandler(error_handler)

# 2. Kết nối đến MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client['TikiProductions']
collection = db['Productions']


# 3. Hàm chuẩn hóa mô tả sản phẩm
def normalize_description(description):
    soup = BeautifulSoup(description, "html.parser")
    description = soup.get_text()
    description = description.strip()
    description = description.lower()
    description = re.sub(r'[^\w\s]', '', description)
    return description


# 4. Đọc danh sách ID sản phẩm từ file CSV
csv_file_path = "products-0-200000(in).csv"
product_ids_df = pd.read_csv(csv_file_path)


# 5. Gửi yêu cầu API và lưu dữ liệu vào MongoDB
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

            # Ghi log sản phẩm vừa thêm thành công
            logging.info(f"Sản phẩm {product_id} đã được thêm vào database.")
        else:
            error_msg = f"Lỗi khi lấy dữ liệu sản phẩm {product_id}. Mã trạng thái: {response.status_code}"
            print(error_msg)
            error_logger.error(error_msg)

    except Exception as e:
        error_msg = f"Lỗi khi lấy dữ liệu sản phẩm {product_id}: {e}"
        print(error_msg)
        error_logger.error(error_msg)


# 6. Chạy tuần tự từng sản phẩm
for product_id in product_ids_df['id']:
    fetch_and_save_product_data(product_id)
