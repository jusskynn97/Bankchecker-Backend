from fastapi import FastAPI, File, UploadFile, HTTPException
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from mbbankchecker import MBBank
import pandas as pd
import time
import unicodedata
import re
import os
from typing import List, Dict
import uuid
import shutil
from pathlib import Path
from dotenv import load_dotenv


load_dotenv()
app = FastAPI()

origins = [
    "http://localhost:3000",
    # Add other origins if needed
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def remove_accents(text):
    """Loại bỏ dấu tiếng Việt và chuyển về chữ thường"""
    if pd.isna(text):
        return ""
    text = str(text)
    text = unicodedata.normalize('NFD', text)
    text = ''.join(char for char in text if unicodedata.category(char) != 'Mn')
    text = re.sub(r'\s+', ' ', text.lower().strip())
    return text

def compare_names(name1, name2):
    """So sánh 2 tên không phân biệt hoa thường và dấu"""
    name1_clean = remove_accents(name1)
    name2_clean = remove_accents(name2)
    return name1_clean == name2_clean

def clean_account_number(account_number):
    """Làm sạch số tài khoản - loại bỏ .0 và khoảng trắng"""
    if pd.isna(account_number):
        return ""
    account_str = str(account_number)
    if account_str.endswith('.0'):
        account_str = account_str[:-2]
    account_str = re.sub(r'[^\d]', '', account_str)
    return account_str.strip()

bank_map = {
    "314": "970448",  # VIB - NH Quốc tế VIB
    "203": "970436",  # Vietcombank
    "204": "970405",  # Agribank
    "201": "970415",  # Vietinbank
    "202": "970418",  # BIDV
    "311": "970422",  # Quân đội (MB)
    "310": "970407",  # Techcombank
    "309": "970432",  # VPBank
    "303": "970403",  # Sacombank
    "358": "970423",  # TPBank
    "302": "970425",  # Hàng Hải (MSB)
    "334": "970429",  # Sài Gòn (SCB)
    "333": "970449",  # Phương đông (OCB)
    "348": "970441",  # Sài Gòn - Hà Nội (SHB)
    "304": "970406",  # Đông Á
    "321": "970433",  # HDBank
    "317": "970440",  # SeABank
    "357": "970414",  # Bưu điện Liên Việt (LPBank)
    "616": "970458",  # Shinhan
    "305": "970431",  # Eximbank
    "323": "970428",  # An Bình (ABBank)
    "617": "452418",  # HSBC Việt Nam
    "313": "970409",  # Bắc Á
    "308": "970438",  # Sài Gòn Công Thương (Saigonbank)
    "355": "970426",  # Việt Á
    "307": "970416",  # Á Châu (ACB)
    "359": "970439",  # Bảo Việt
    "356": "970437",  # Việt Nam Thương Tín (VietBank)
    "341": "970412",  # Xăng Dầu Petrolimex (PG Bank)
    "319": "970417",  # Oceanbank
    "320": "970424",  # GPBank
    "353": "970454",  # Kiên Long
    "306": "970421",  # Nam Á
    "352": "970453",  # NCB
    "360": "970450",  # Pvcombank
    "327": "970451",  # Bản Việt (Viet Capital Bank)
    "339": "970444",  # Xây Dựng (CBBank)
    "661": "970455",  # CIMB Bank
    "207": "970479",  # Chính sách xã hội (VBSP)
    "208": "970430",  # Phát triển Việt Nam (VDB)
    "604": "970410",  # Standard Chartered
    "603": "970456",  # Hong Leong Việt Nam
    "501": "970459",  # Public Bank
    "665": "970457",  # UOBVN
    "663": "970452",  # Woori Bank
    "502": "970434",  # NH Indovina
    "309A": "546034", # NH số CAKE by VPBank
    "309B": "963388", # NH số Ubank by VPBank
    "353A": "970499", # NH số UMEE by KienlongBank
    "TM01": "970451", # Ngân hàng số Timo (Bản Việt Bank)
    "DG04": "970449", # Ngân hàng số Lio (OCB Bank)
}

@app.get("/")
def home():
    return {"message": os.getenv("ACCOUNT")}

@app.post("/check-file")
async def check_file(file: UploadFile = File(...)):
    # Validate file type
    if not file.filename.endswith(('.xlsx', '.xls')):
        raise HTTPException(status_code=400, detail="Only Excel files (.xlsx, .xls) are allowed")

    # Create temporary directory
    temp_dir = Path("temp_files")
    temp_dir.mkdir(exist_ok=True)
    file_path = temp_dir / f"{uuid.uuid4()}_{file.filename}"

    try:
        # Save uploaded file
        with open(file_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)

        # Read and process Excel file, skipping first 3 rows
        df = pd.read_excel(file_path, skiprows=3)
        
        # Filter valid rows
        bank_column_index = 5
        account_column_index = 2
        df_filtered = df[
            (df.iloc[:, 1].notna()) &
            (df.iloc[:, 1] != '') &
            (df.iloc[:, account_column_index].notna()) &
            (df.iloc[:, account_column_index] != '') &
            (df.iloc[:, bank_column_index].notna()) &
            (df.iloc[:, bank_column_index] != '')
        ].copy()

        df_filtered['Ma_Ngan_Hang'] = df_filtered.iloc[:, bank_column_index].astype(str).str.split('-').str[0].str.strip()

        # Initialize results
        results = {"valid": [], "invalid": []}

        # Initialize MBBank
        mb = MBBank(username=os.getenv("ACCOUNT"), password=os.getenv("PASSWORD"))

        # Process each account
        for index, row in df_filtered.iterrows():
            ma_ngan_hang = str(row['Ma_Ngan_Hang']).strip()
            so_tai_khoan = clean_account_number(row.iloc[account_column_index])
            ten_nguoi_nhan = str(row.iloc[1]).strip()
            row_number = index + 4  # Adjust for 1-based indexing and 3 skipped rows

            # Skip if critical data is missing
            if not ma_ngan_hang or ma_ngan_hang == 'nan' or not so_tai_khoan or so_tai_khoan == 'nan' or not ten_nguoi_nhan:
                results["invalid"].append({
                    "account": {
                        "accountNumber": so_tai_khoan or "N/A",
                        "accountName": ten_nguoi_nhan or "N/A",
                        "bankName": str(row.iloc[bank_column_index]) or "N/A",
                        "row": row_number
                    },
                    "errors": ["Missing required data (account number, name, or bank code)"]
                })
                continue

            account_info = {
                "accountNumber": so_tai_khoan,
                "accountName": ten_nguoi_nhan,
                "bankName": str(row.iloc[bank_column_index]),
                "row": row_number
            }

            if ma_ngan_hang in bank_map:
                bank_code = bank_map[ma_ngan_hang]
                transfer_type = "INHOUSE" if bank_code == "970422" else "FAST"

                try:
                    result = mb.inquiryAccountName(
                        creditAccount=so_tai_khoan,
                        creditAccountType="ACCOUNT",
                        transfer_type=transfer_type,
                        bankCode=bank_code
                    )

                    if 'benName' in result and result['benName']:
                        is_name_match = compare_names(ten_nguoi_nhan, result['benName'])
                        if is_name_match:
                            results["valid"].append(account_info)
                        else:
                            results["invalid"].append({
                                "account": account_info,
                                "errors": [f"Account name mismatch (Bank: {result['benName']})"]
                            })
                    else:
                        results["invalid"].append({
                            "account": account_info,
                            "errors": ["No account name returned from bank"]
                        })

                except Exception as e:
                    results["invalid"].append({
                        "account": account_info,
                        "errors": [f"Account verification failed: {str(e)}"]
                    })
            else:
                results["invalid"].append({
                    "account": account_info,
                    "errors": [f"Unknown bank code: {ma_ngan_hang}"]
                })

            time.sleep(1)  # API rate limiting as per provided script

        return JSONResponse(content=results)

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error processing file: {str(e)}")

    finally:
        # Clean up
        if file_path.exists():
            os.remove(file_path)